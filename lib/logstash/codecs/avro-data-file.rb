# encoding: utf-8

require 'avro'
require 'logstash/codecs/base'
require 'logstash/namespace'
require 'tmpdir'

# This  codec will append a string to the message field
# of an event, either in the decoding or encoding methods
#
# This is only intended to be used as an example.
#
# input {
#   stdin { codec => 'avro-data-file' }
# }
#
# or
#
# output {
#   stdout { codec => 'avro-data-file' }
# }
#
class LogStash::Codecs::AvroDataFile < LogStash::Codecs::Base

  config_name 'avro-data-file'

  # Set the directory where logstash will store the tmp files before processing them.
  # default to the current OS temporary directory in linux /tmp/logstash/avro
  config :temporary_directory, validate: :string, default: File.join(Dir.tmpdir, 'logstash', 'avro')

  def register
    require 'fileutils'
    FileUtils.mkdir_p(temporary_directory) unless Dir.exist?(temporary_directory)
    reset
  end

  def decode(data)
    merge(data)
  end

  def flush
    tempfile.flush
    return unless block_given?

    Avro::DataFile.open(tempfile.path, 'r') do |reader|
      reader.each do |avro_message|
        yield LogStash::Event.new(avro_message)
      end
    end
  rescue => e
    @logger.error('Avro parse error', error: e)
  ensure
    reset
  end

  def encode(_event)
    raise 'Not implemented'
  end

  private

  attr_accessor :tempfile
  attr_reader :temporary_directory

  def merge(bytes)
    tempfile.write(bytes)
  end

  def reset
    unless tempfile.nil?
      begin
        File.unlink(tempfile.path)
        tempfile.close
      rescue Errno::ENOENT # rubocop:disable Lint/HandleExceptions
      end
    end
    self.tempfile = Tempfile.create('', temporary_directory)
  end
end
