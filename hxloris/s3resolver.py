# -*- coding: utf-8 -*-
#
# s3resolver copied from:
#   https://github.com/Harvard-ATG/loris/blob/development/loris/s3resolver.py
#

import boto3
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
    """Resolver for image files stored on aws s3 buckets.

    The first call to `resolve()` copies the source image into a local cache;
    subsequent calls use local copy from the cache.

    A config example:

        [resolver]
        impl = 'hxloris.s3resolver.S3Resolver'

        # absolute path to dir where source images are downloaded from s3
        # mandatory
        cache_root = '/var/loris_cache_root'

        # subsection to define mappings from :ident to an s3 bucket/key
        # optional
        [[bucket_map]]
          [[[site1]]]
            bucket = 'bucket-for-site1'
            key_prefix = 'loris/images'

          [[[site2]]]
            bucket = 'bucket-for-site2'
            key_prefix = 'loris/other-images'

        ...

    an incoming request url and its corresponding s3 bucket/prefix:
        http://localhost/site1/this/that/image.jpg/:region/:size/:rotation/default.jpg
        s3://bucket-for-site1/loris/images/this/that/image.jpg
    or
        http://localhost/site3/blah/image3.jpg/:region/:size/:rotation/default.jpg
        s3://site3/blah/image3.jpg

    `bucket_map` is optional (as is `key_prefix`), but will always require a
    `bucket` to be in the request url. For example, the url below is invalid:
        http://localhost/image.jpg

    If it looks too similar to loris.resolver.SimpleHTTPResolver... you're right!
    """

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

        # if not in us-east-1, set envvar AWS_DEFAULT_REGION to avoid extra
        # requests when downloading from s3
        session = boto3.session.Session()  # to be thread safe
        self.s3 = session.resource('s3')

        logger.info('loaded s3 resolver with config: {}'.format(config))


    def is_resolvable(self, ident):
        """ checks if ident contains a readable s3 object.

        this generates a head request for the s3 object
        """
        ident = unquote(ident)

        if not self._ident_regex_checker.is_allowed(ident):
            return False

        fp = self.cache_dir_path(ident=ident)
        if os.path.exists(fp):
            return True
        else:
            try:
                (bucketname, keyname) = self.s3bucket_from_ident(ident)
            except ResolverException as e:
                logger.warn(e)
                return False

            # check that we can get to this object on s3
            # access to s3obj prop generates a head request or 404
            try:
                s3obj = self.s3.Object(bucketname, keyname)
                content_length = s3obj.content_length
            except Exception as e:
                logger.error('unable to access s3 object ({}:{}): {}'.format(
                    bucketname, keyname, e))
                return False
            else:
                if content_length > 0:
                    return True
                else:
                    logger.warning('empty s3 object ({}:{})'.format(
                        bucketname, keyname))
                    return False


    def get_format(self, ident, potential_format):
        if self.default_format is not None:
            return self.default_format
        elif potential_format is not None:
            return potential_format
        else:
            return self.format_from_ident(ident)


    def s3bucket_from_ident(self, ident):
        """ returns tuple(buckename, keyname) parsed from ident."""
        key_parts = ident.split('/', 1)
        if len(key_parts) == 2:
            (bucket, partial_key) = key_parts
        else:
            raise ResolverException(
                'Invalid identifier. Expected bucket/ident; got {}'.format(
                    key_parts))

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
            CacheNamer.cache_directory_name(ident=ident))


    def cached_file_for_ident(self, ident):
        # recover filepath for ident in cache
        cache_dir = self.cache_dir_path(ident)
        if os.path.exists(cache_dir):
            files = glob.glob(os.path.join(cache_dir, 'loris_cache.*'))
            if files:
                return files[0]
        return None


    def cache_file_extension(self, ident, content_type=None):
        if content_type is not None:
            try:
                extension = self.get_format(
                    ident,
                    constants.FORMATS_BY_MEDIA_TYPE[content_type]
                )
            except KeyError:
                logger.warn(
                    'wonky s3 resource content-type({}) for ident({})',
                    content_type, ident)
                # Attempt without the content-type
                extension = self.get_format(ident, None)
        else:
            extension = self.get_format(ident, None)
        return extension


    def copy_to_cache(self, ident):
        """ downloads image source file from s3, if not in cache already."""
        ident = unquote(ident)

        # get source image and write to temporary file
        (bucketname, keyname) = self.s3bucket_from_ident(ident)

        try:
            s3obj = self.s3.Object(bucketname, keyname)
            content_type = s3obj.content_type
        except Exception as e:
            msg = 'no content_type for s3 object ({}:{}): {}'.format(
                bucketname, keyname, e)
            logger.error(msg)
            raise ResolverException(msg)

        extension = self.cache_file_extension(ident, content_type)
        cache_dir = self.cache_dir_path(ident)
        mkdir_p(cache_dir)
        local_fp = os.path.join(cache_dir, "loris_cache." + extension)
        with tempfile.NamedTemporaryFile(
                dir=cache_dir, delete=False) as tmp_file:
            try:
                self.s3.Bucket(bucketname).download_fileobj(keyname, tmp_file)
            except Exception as e:
                msg = 'unable to access or save s3 object ({}:{}): {}'.format(
                    bucketname, keyname, e)
                logger.error(msg)
                raise ResolverException(msg)

        # Now rename the temp file to the desired file name if it still
        # doesn't exist (another process could have created it).
        #
        # Note: This is purely an optimisation; if the file springs into
        # existence between the existence check and the copy, it will be
        # overridden.
        if os.path.exists(local_fp):
            logger.info(
                'Another process downloaded src image {}'.format(local_fp))
            os.remove(tmp_file.name)
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
        local_rules_fp = os.path.join(
            cache_dir, 'loris_cache.' + self.auth_rules_ext)
        try:
            self.s3.Object(bucketname, rules_keyname).download_file(
                local_rules_fp)
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

