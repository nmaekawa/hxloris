# Sample Test passing with nose and pytest

from hxloris.s3resolver import S3Resolver


config = {
    u'impl': 'hxloris.s3resolver.S3Resolver',
    u'cache_root': '/var/tmp/loris',
    u'user_extra_info': False,
    u'bucket_map':
    {
        "iiif": {
            "bucket": "bucket-iiif",
            "key_prefix": "hx"
        },
        "loris": {
            "bucket": "bucket-loris"
        }
    }
}


def test_config_bucket_map():
    r = S3Resolver(config)

    assert r.has_bucket_map
    assert 'iiif' in r.bucket_map
    assert r.bucket_map['iiif']['bucket'] == 'bucket-iiif'


def test_bucket_from_ident_ok():
    r = S3Resolver(config)

    b, k = r.s3resource_ident(
        'iiif/image.jpg/region/size/rotation/default.jpg')
    assert b == 'bucket-iiif'
    assert k == 'hx/image.jpg/region/size/rotation/default.jpg'

    b, k = r.s3resource_ident(
        'its_not_a_bucket/image.jpg/region/size/rotation/default.jpg')
    assert b == 'its_not_a_bucket'


def test_config_no_bucket_map():
    del config[u'bucket_map']
    r = S3Resolver(config)

    assert not r.has_bucket_map
    assert getattr(r, 'bucket_map', None) is None

    b, k = r.s3resource_ident(
        'iiif/image.jpg/region/size/rotation/default.jpg')
    assert b == 'iiif'



