#!/bin/env ruby

require 'nats/io/client'
require 'logger'
require 'uri'
require 'timeout'
require 'pp'

@logger = Logger.new(STDOUT)

def convertimg(job, output)
  Timeout.timeout(30) do
    uri = URI(job["image"])

    @logger.info("Processing %s" % job.inspect)

    out = File.join(output, "%s%s" % [job["id"], File.extname(uri.path)])

    @logger.info("Converting %s into %s" % [uri.path, out])
    system("/bin/convert", "-monochrome", uri.path, out) || raise("convert failed")
  end
end

def process(nc, output)
  @logger.info("Looking for a B&W conversion job")

  begin
    msg = nc.old_request("$JS.API.CONSUMER.MSG.NEXT.IMAGES.BW", "", timeout: 30)
  rescue NATS::IO::Timeout
    return
  end

  job = JSON.parse(msg.data)

  convertimg(job, output)

  nc.publish(job["advisory"], {"id" => job["id"]}.to_json) if job["advisory"]

  nc.publish(msg.reply, "")
end

nats = NATS::IO::Client.new

nats.connect(ENV.fetch("NATS_URL", "localhost"))

out_dir = ENV["OUTDIR"] || abort("Please set OUTDIR")

while true
  process(nats, out_dir)
end
