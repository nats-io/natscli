# to create a replicated bucket
nats obj add FILES --replicas 3

# store a file in the bucket
nats obj put FILES image.jpg

# store contents of STDIN in the bucket
cat x.jpg|nats obj put FILES --name image.jpg

# retrieve a file from a bucket
nats obj get FILES image.jpg -O out.jpg

# delete a file
nats obj del FILES image.jpg

# delete a bucket
nats obj del FILES

# view bucket info
nats obj info FILES

# view file info
nats obj info FILES image.jpg

# list known buckets
nats obj ls

# view all files in a bucket
nats obj ls FILES

# prevent further modifications to the bucket
nats obj seal FILES

# create a bucket backup for FILES into backups/FILES
nats obj status FILES
nats stream backup <stream name> backups/FILES

# restore a bucket from a backup
nats stream restore <stream name> backups/FILES
