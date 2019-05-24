# -*- coding: utf-8 -*-
import boto
import json
import logging
import os
from urllib.parse import unquote

from loris.resolver import _AbstractResolver
from loris.loris_exception import ResolverException


_DEFAULT_SINGLE_BUCKET_CUE = 'loris'
logger = logging.getLogger(__name__)


class S3Resolver(_AbstractResolver):
    """Resolver for images coming from AWS S3 bucket.

    The config dictionary MUST contain
     * `cache_root`, which is the absolute path to the directory where source images
        should be cached.
    """
    def __init__(self, config):
        super(S3Resolver, self).__init__(config)

        logger.debug('loaded s3resolver with config: %s' % config)

        if 'cache_root' in self.config:
            self.cache_root = self.config['cache_root']
        else:
            message = ('Server Side Error: Configuration incomplete and '
                       'cannot resolve. Missing setting for cache_root.')
            logger.error(message)
            raise ResolverException(500, message)

        self.has_bucket_map = False
        if 'bucket_map' in config:
            try:
                self.bucket_map = json.loads(config['bucket_map'])
            except Exception as e:
                logger.error(
                    'unable to parse bucket_map from config: {}'.format(
                        config['bucket_map']))
            else:
                self.has_bucket_map = True
                logger.info('s3resolver with bucket_map: {}'.format(
                    self.bucket_map))


    @staticmethod
    def format_from_ident(ident):
        return ident.split('.')[-1]


    @staticmethod
    def create_directory_if_not_exists(path):
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            logger.debug('Attempting to create dir({}) '.format(directory))
            os.makedirs(directory, mode=0o755)


    def is_resolvable(self, ident):
        ident = unquote(ident)
        local_fp = os.path.join(self.cache_root, ident)
        if os.path.exists(local_fp):
            return True
        else:
            # check that we can get to this object on S3
            s3 = boto.connect_s3()

            bucketname, keyname = self.s3bucket_from_ident(ident)
            try:
                bucket = s3.get_bucket(bucketname)
            except boto.exception.S3ResponseError as e:
                logger.error(e)
                return False

            if bucket.get_key(keyname):
                return True
            else:
                logger.warning('invalid key({}) for bucket ({})'.format(
                    keyname, bucketname))
                return False


    def resolve(self, ident):
        ident = unquote(ident)
        local_fp = os.path.join(self.cache_root, ident)
        format = self.format_from_ident(ident)
        logger.debug('ident({}) local_fp({}) format({})'.format(ident, local_fp))

        bucketname, keyname = self.s3bucket_from_ident(ident)

        if os.path.exists(local_fp):
            logger.debug('src image from local disk: %s' % (local_fp,))
            return (local_fp, format)
        else:
            # get image from S3
            logger.debug('pulling from s3 bucket:key = %s, %s' % (bucketname, keyname))

            s3 = boto.connect_s3()
            bucket = s3.get_bucket(bucketname)
            key = bucket.get_key(keyname)

            # Need local_fp directory to exist before writing image to it
            self.create_directory_if_not_exists(local_fp)
            try:
                key.get_contents_to_filename(local_fp)
            except boto.exception.S3ResponseError as e:
                # TODO: will return anyway... is this right?
                logger.warn(e)

            return (local_fp, format)



    def s3bucket_from_ident(self, ident):
        """Returns a tuple (bucket, key) parsed from the ident

        For example, if the following URL is received by loris:

        http://host/:identifier/:region/:size/:rotation/default.jpg

        Then it will be parsed as follows:

            ident = "mybucket/images/1/foo.jpg"
            bucketname = "mybucket"
            keyname = "images/1/foo.jpg"
        """
        key_parts = ident.split('/', 1)
        if len(key_parts) == 2:
            (bucketname, keyname) = key_parts
        else:
            raise ResolverException(
                500,
                'Invalid identifier. Expected bucket/ident; got {}'.format(
                    key_parts)
            )

        # check if bucketname actually means something different
        if (self.has_bucket_map and bucketname in self.bucket_map):
            bucketname = self.bucket_map[bucketname]

        return bucketname, keyname

