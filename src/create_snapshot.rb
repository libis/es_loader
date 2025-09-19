
#encoding: UTF-8
#
# Monthly full snapshot in new repo 
# Daily incremental snapshot
#

require 'yaml'

$LOAD_PATH << '.' << './lib'
require 'logger'
require_relative './lib/loader'

begin

  #Create a loader object.
  loader = Loader.new()
  loader_config = loader.config()

  pp loader.config_file
  config = YAML::load_file("#{File.dirname(__FILE__)}/../config/#{loader.config_file}", permitted_classes: [Symbol, Regexp])
  
  if config[:snapshot_index_pattern].nil?
    raise "config[:snapshot_index_pattern] are not defined in #{loader.config_file}"
    exit(0)
  end
  
  if config[:snapshot_repo].nil?
    raise "config[:snapshot_repo] are not defined in #{loader.config_file}"
    exit(0)
  end

  if config[:snapshot_repo_path].nil?
    raise "config[:snapshot_repo_path] are not defined in #{loader.config_file}"
    exit(0)
  end

  loader.logger.info "use utils for es_cluster : #{loader.es_cluster}"
  loader.check_elastic()

  @es_url = config[:es_url]

 #  @es_url = "https://admin:iadmindid@host.docker.internal:9300/"

    @es_client = Elasticsearch::Client.new url: @es_url, transport_options: {  ssl:  { verify: false } }

  # Configuration
  month_str = Time.now.strftime('%Y-%m')
  repo_name = "#{config[:snapshot_repo]}_#{month_str}"
  snapshot_name = "snap-icandid-#{Time.now.strftime('%Y%m%d_%H%M')}"
  index_pattern = config[:snapshot_index_pattern]
  
  # Repository settings
  repo_settings = {
    type: 'fs',
    settings: {
      location: File.join(config[:snapshot_repo_path], repo_name),
      compress: true
    }
  }

  # Create repository if it doesn't exist
  begin
    existing_repos = @es_client.snapshot.get_repository rescue {}
    unless existing_repos.key?(repo_name)
      @es_client.snapshot.create_repository(repository: repo_name, body: repo_settings)
      puts "Created repository: #{repo_name}"
    else
      puts "Repository #{repo_name} already exists."
    end
  rescue => e
    puts "Error checking or creating repository: #{e.message}"
  end


  #Check if all snapshots are processed
  begin
    snapshots = @es_client.snapshot.get(repository: repo_name, snapshot: '_all')
    incomplete = snapshots['snapshots'].select { |s| s['state'] != 'SUCCESS' }

    if incomplete.empty?
      puts "✅ All snapshots in repository '#{repo_name}' completed successfully."
    else
      puts "⚠️ Some snapshots are not successful:"
      incomplete.each do |snap|
        puts "- #{snap['snapshot']}: #{snap['state']}"
      end
      exit()
    end
  rescue => e
    puts "Error checking snapshots: #{e.message}"
    exit()
  end




  # Create snapshot
  begin
    snapshot_body = {
      indices: "#{index_pattern}",
      include_global_state: false
    }
    puts "Snapshot #{snapshot_name} creating in repository #{repo_name}."
    response = @es_client.snapshot.create(
      repository: repo_name,
      snapshot: snapshot_name,
      body: snapshot_body,
      wait_for_completion: true
    )
    puts "Snapshot #{snapshot_name} created in repository #{repo_name}."
  rescue => e
    puts "Error creating snapshot: #{e.message}"
  end

end
