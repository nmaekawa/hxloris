# -*- coding: utf-8 -*-
#
# s3resolver copied from:
#   https://github.com/Harvard-ATG/loris/blob/development/loris/s3resolver.py
#

import boto
import glob
import logging
import os
import tempfile
from urllib.parse import unquote

from loris import constants
from loris.identifiers import CacheNamer
from loris.identifiers import IdentRegexChecker
from loris.loris_exception import ResolverException
from loris.resolver import _AbstractResolver
from loris.utils import mkdir_p
from loris.utils import safe_rename
from loris.img_info import ImageInfo


logger = logging.getLogger(__name__)


class S3Resolver(_AbstractResolver):
    '''Resolver for image files stored on aws s3 buckets.

    The first call to `resolve()` copies the source image into a local cache;
    subsequent calls use local copy from the cache.

    The config dictionary MUST contain
     * `cache_root`, which is the absolute path to the directory where source
       images should be cached.

    The config dictionary MAY contain

    '''
    def __init__(self, config):
        super(S3Resolver, self).__init__(config)
        self.default_format = self.config.get('default_format', None)

        self._ident_regex_checker = IdentRegexChecker(
            ident_regex=self.config.get('ident_regex')
        )
        self._cache_namer = CacheNamer()

        if 'cache_root' in self.config:
            self.cache_root = self.config['cache_root']
        else:
            message = ('Server Side Error: Configuration incomplete and '
                       'cannot resolve. Missing setting for cache_root.')
            logger.error(message)
            raise ResolverException(message)

        self.has_bucket_map = False
        if 'bucket_map' in config:
            self.bucket_map = config['bucket_map']
            self.has_bucket_map = True
            logger.debug('s3 bucket_map: {}'.format(self.bucket_map))

        logger.debug('loaded s3 resolver with config: {}'.format(config))


    def is_resolvable(self, ident):
        # request ident and return true if response status is less than 400
        # check ident against regex
        ident = unquote(ident)

        if not self._ident_regex_checker.is_allowed(ident):
            return False

        fp = self.cache_dir_path(ident=ident)
        if os.path.exists(fp):
            return True
        else:
            try:
                (bucketname, keyname) = self.s3resource_ident(ident)
            except ResolverException as e:
                logger.warn(e)
                return False

            # check that we can get to this object on s3
            s3 = boto.connect_s3()
            try:
                bucket = s3.get_bucket(bucketname)
            except boto.exception.S3ResponseError as e:
                logger.error(e)
                return False
            else:
                k = bucket.get_key(keyname)
                if k is None:
                    logger.warning('invalid key({}) for bucket({})'.format(
                        keyname, bucketname))
                    return False
                return True


    def get_format(self, ident, potential_format):
        # always return default_format, but if none, returns potential_format
        # and if none, then tries to get format_from_ident()
        if self.default_format is not None:
            return self.default_format
        elif potential_format is not None:
            return potential_format
        else:
            return self.format_from_ident(ident)


    def s3resource_ident(self, ident):
        key_parts = ident.split('/', 1)
        if len(key_parts) == 2:
            (bucket, partial_key) = key_parts
        else:
            raise ResolverException(
                'Invalid identifier. Expected bucket/ident; got {}'.format(
                    key_parts)
            )

        # check if bucketname actually means something different
        if (self.has_bucket_map and bucket in self.bucket_map):
            bucketname = self.bucket_map[bucket]['bucket']
            if 'key_prefix' in self.bucket_map[bucket]:
                keyname = os.path.join(
                    self.bucket_map[bucket]['key_prefix'],
                    partial_key)
            else:
                keyname = partial_key
            return (bucketname, keyname)

        else:  # what came in ident is the actual bucketname
            return (bucket, partial_key)


    def cache_dir_path(self, ident):
        # build dir path for ident file in cache
        return os.path.join(
            self.cache_root,
            CacheNamer.cache_directory_name(ident=ident)
        )

    def raise_404_for_ident(self, ident):
        raise ResolverException("Image not found for identifier: %r." % ident)

    def cached_file_for_ident(self, ident):
        # recover filepath for ident in cache
        cache_dir = self.cache_dir_path(ident)
        if os.path.exists(cache_dir):
            files = glob.glob(os.path.join(cache_dir, 'loris_cache.*'))
            if files:
                return files[0]
        return None

    def cache_file_extension(self, ident, key_object):
        if hasattr(key_object, 'content_type'):
            try:
                extension = self.get_format(
                    ident,
                    constants.FORMATS_BY_MEDIA_TYPE[key_object.content_type]
                )
            except KeyError:
                logger.warn(
                    'wonky s3 resource content-type({}) for ident({})',
                    key_object.content_type, ident)
                # Attempt without the content-type
                extension = self.get_format(ident, None)
        else:
            extension = self.get_format(ident, None)
        return extension

    def copy_to_cache(self, ident):
        ident = unquote(ident)

        # get source image and write to temporary file
        (bucketname, keyname) = self.s3resource_ident(ident)
        assert bucketname is not None

        cache_dir = self.cache_dir_path(ident)
        mkdir_p(cache_dir)

        s3 = boto.connect_s3()
        bucket = s3.get_bucket(bucketname)
        key = bucket.get_key(keyname)
        if key is None:
            msg = 'Source image({}:{}) not found for identifier({})'.format(
                bucketname, keyname, ident)
            logger.warn(msg)
            raise ResolverException(msg)

        extension = self.cache_file_extension(ident, key)
        local_fp = os.path.join(cache_dir, "loris_cache." + extension)

        with tempfile.NamedTemporaryFile(
                dir=cache_dir, delete=False) as tmp_file:
            key.get_contents_to_file(tmp_file)

        # Now rename the temp file to the desired file name if it still
        # doesn't exist (another process could have created it).
        #
        # Note: This is purely an optimisation; if the file springs into
        # existence between the existence check and the copy, it will be
        # overridden.
        if os.path.exists(local_fp):
            logger.info(
                'Another process downloaded src image {}'.format(local_fp))
            os.path.remove(tmp_file.name)
        else:
            safe_rename(tmp_file.name, local_fp)
            logger.info("Copied {}:{} to {}".format(
                bucketname, keyname, local_fp))

        # Check for rules file associated with image file
        # These files are < 2k in size, so fetch in one go.
        # Assumes that the rules will be next to the image
        # cache_dir is image specific, so this is easy
        bits = os.path.split(keyname)  # === bash basename
        fn = bits[1].rsplit('.')[0] + "." + self.auth_rules_ext
        rules_keyname = bits[0] + '/' + fn
        try:
            rules_key = bucket.get_key(rules_keyname)
            if rules_key is not None:
                local_rules_fp = os.path.join(
                    cache_dir, 'loris_cache.' + self.auth_rules_ext)
                if not os.path.exists(local_rules_fp):
                    rules_key.get_contents_to_filename(local_rules_fp)
        except Exception as e:
            # no connection available?
            msg = 'ignoring rules file({}/{}) for ident({}): {}'.format(
                   bucketname, rules_keyname, ident, e)
            logger.warn(msg)

        return local_fp


    def resolve(self, app, ident, base_uri):
        cached_file_path = self.cached_file_for_ident(ident)
        if not cached_file_path:
            cached_file_path = self.copy_to_cache(ident)
        format_ = self.get_format(cached_file_path, None)
        uri = self.fix_base_uri(base_uri)
        if self.use_extra_info:
            extra = self.get_extra_info(ident, cached_file_path)
        else:
            extra = {}
        return ImageInfo(app, uri, cached_file_path, format_, extra)

