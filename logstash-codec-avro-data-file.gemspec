Gem::Specification.new do |s|
  s.name          = 'logstash-codec-avro-data-file'
  s.version       = '0.1.0'
  s.licenses      = ['MIT']
  s.summary       = 'Codec for parsing avro data files'
  s.homepage      = 'https://github.com/salsify/logstash-codec-avro-data-file'
  s.authors       = ['Kyle Phelps']
  s.email         = 'kphelps@salsify.com'
  s.require_paths = ['lib']

  s.files = Dir[
    'lib/**/*',
    'spec/**/*',
    'vendor/**/*',
    '*.gemspec',
    '*.md',
    'CONTRIBUTORS',
    'Gemfile',
    'LICENSE',
    'NOTICE.TXT'
  ]
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { 'logstash_plugin' => 'true', 'logstash_group' => 'codec' }

  s.add_runtime_dependency 'avro'
  s.add_runtime_dependency 'logstash-codec-line'
  s.add_runtime_dependency 'logstash-core-plugin-api', '~> 2.0'
  s.add_development_dependency 'logstash-devutils'

  s.add_development_dependency 'bundler', '~> 1.16'
  s.add_development_dependency 'overcommit'
  s.add_development_dependency 'salsify_rubocop', '~> 0.48.0'
end
