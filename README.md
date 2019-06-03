hx add-ons to loris image server
===============================

Overview
--------

hx add-ons to [loris image server](https://github.com/loris-imageserver/loris).

Installation
------------

Clone the repo and install via pip:

    $> git clone https://github.com/nmaekawa/hxloris.git
    $> cd hxloris
    $> pip install -r requirements.txt

If you already have [loris](https://github.com/loris-imageserver/loris)
installed, then you can do:

    $> pip install git+git://github.com/loris-imageserver/loris.git@9069177


This resolver uses [boto3](https://github.com/boto/boto3).

Usage
------

In your loris configuration do something like below:

    
    ...
    [resolver]
    impl = 'hxloris.s3resolver.S3Resolver'
    cache_root = '/var/loris_cache_root'

    [[bucket_map]]
        [[[bucket_placeholder]]]
        bucket = 'my-s3-bucket'
        key_prefix = 'loris/images'

    ...
    

For requests like:

    http://localhost/bucket_placeholder/this/that/image.jpg/:region/:size/:rotation/default.jpg


The resolver will download the source image from

    s3://my-s3-bucket/loris/images/this/that/image.jpg


When the bucket placeholder name does not match any key in the `bucket_map`
configuration, or if `bucket_map` is omitted, then whatever comes in the url is
considered the bucket name. For example:

    http://localhost/bucket-x/this/that/image.jpg/:region/:size/:rotation/default.jpg

will result in:

    s3://bucket-x/this/that/image.jpg


If the s3 objects are not in the default AWS region (us-east-1) then configure
the environment variable AWS_DEFAULT_REGION to avoid extra requests (for boto3
to figure out the correct aws region).

---eop
