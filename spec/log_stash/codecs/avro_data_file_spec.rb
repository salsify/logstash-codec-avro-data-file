# encoding: utf-8

require 'logstash/codecs/avro-data-file'
require 'json'

describe LogStash::Codecs::AvroDataFile do
  let(:config) { {} }
  let(:codec) { described_class.new(config).tap(&:register) }
  let(:lines) { [] }
  let(:events) { [] }
  let(:expected_events) { [] }

  before do
    lines.each(&codec.method(:decode))
  end

  shared_examples "produces the correct output" do
    specify do
      expect(events.length).to eq expected_events.length
      expect(events).to all(be_an_instance_of(LogStash::Event))
      event_hashes = events.map(&:to_hash).map do |event|
        event.except('@timestamp', '@version')
      end
      expected_hashes = expected_events.map(&:to_hash).map do |event|
        event.except('@timestamp', '@version')
      end

      expect(event_hashes).to eq expected_hashes
    end
  end

  describe "#decode" do

    context "without flushing" do
      let(:lines) { ['test', 'test2'] }

      include_examples "produces the correct output"
    end

    context "with a flush" do

      before do
        codec.flush do |event|
          events << event
        end
      end

      context "invalid data" do
        let(:lines) { ['test', 'test2'] }

        include_examples "produces the correct output"
      end

      context "valid data" do
        let(:schema_name) { 'mock_schema' }
        let(:schema_type) { 'record' }
        let(:schema_namespace) { 'com.salsify.test' }
        let(:schema) do
          {
            'type' => schema_type,
            'name' => schema_name,
            'namespace' => schema_namespace,
            'fields' => [
              {
                'name' => 'id',
                'type' => 'long'
              }
            ]
          }.to_json
        end
        let(:avro_ids) { (0...100).to_a }
        let(:avro_messages) do
          avro_ids.map do |id|
            { 'id' => id }
          end
        end
        let(:tempfile) do
          Tempfile.new('restore-test')
        end
        let(:lines) do
          Avro::DataFile.open(tempfile.path, 'w', schema) do |writer|
            avro_messages.each do |message|
              writer << message
            end
          end
          File.open(tempfile.path, 'rb', &:to_a)
        end
        let(:expected_events) { avro_messages.map(&LogStash::Event.method(:new)) }

        include_examples "produces the correct output"

        context "decorate events" do
          let(:config) { { 'decorate_events' => true } }
          let(:expected_events) do
            avro_messages.map do |message|
              LogStash::Event.new(message).tap do |event|
                event.set('[@metadata][avro][type]', schema_type)
                event.set('[@metadata][avro][name]', schema_name)
                event.set('[@metadata][avro][namespace]', schema_namespace)
              end
            end
          end

          include_examples "produces the correct output"
        end
      end
    end
  end
end
