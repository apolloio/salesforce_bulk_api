module SalesforceBulkApi
require 'timeout'

  class Connection
    include Concerns::Throttling

    @@XML_HEADER = '<?xml version="1.0" encoding="utf-8" ?>'
    @@API_VERSION = nil
    @@LOGIN_HOST = 'login.salesforce.com'
    @@INSTANCE_HOST = nil # Gets set in login()

    def initialize(api_version,client)
      @client=client
      @session_id = nil
      @server_url = nil
      @instance = nil
      @@API_VERSION = api_version
      @@LOGIN_PATH = "/services/Soap/u/#{@@API_VERSION}"
      @@PATH_PREFIX = "/services/async/#{@@API_VERSION}/"

      login()
    end

    def login()
      client_type = @client.class.to_s
      case client_type
      when "Restforce::Data::Client"
        @session_id=@client.options[:oauth_token]
        @server_url=@client.options[:instance_url]
      else
        @session_id=@client.oauth_token
        @server_url=@client.instance_url
      end
      @instance = parse_instance()
      if @instance.include? 'cloudforce.com'
        @@INSTANCE_HOST = @instance
      else
        @@INSTANCE_HOST = "#{@instance}.salesforce.com"
      end
    end

    def post_xml(host, path, xml, headers)
      host = host || @@INSTANCE_HOST
      if host != @@LOGIN_HOST # Not login, need to add session id to header
        headers['X-SFDC-Session'] = @session_id
        path = "#{@@PATH_PREFIX}#{path}"
      end
      i = 0
      begin
        count :post
        throttle(http_method: :post, path: path)
        https(host).post(path, xml, headers).body
      rescue
        i += 1
        if i < 3
          puts "Request fail #{i}: Retrying #{path}"
          retry
        else
          puts "FATAL: Request to #{path} failed three times."
          raise
        end
      end
    end

    def get_request(host, path, headers)
      host = host || @@INSTANCE_HOST
      path = "#{@@PATH_PREFIX}#{path}"
      if host != @@LOGIN_HOST # Not login, need to add session id to header
        headers['X-SFDC-Session'] = @session_id;
      end

      count :get
      throttle(http_method: :get, path: path)
      https(host).get(path, headers).body
    end

    def https(host)
      req = Net::HTTP.new(host, 443)
      req.use_ssl = true
      req.verify_mode = OpenSSL::SSL::VERIFY_NONE
      req
    end

    def parse_instance()
      # the original parse_instance function parsed the following incorrectly
      # 'https://td123.my.salesforce.com' => 'https://td12.my.salesforce.com'
      # because of this stupid line:
      # @instance = @server_url.match(/https:\/\/[a-z]{2}[0-9]{1,2}/).to_s.gsub("https://","")
      # which makes incorrect assumptions
      # 
      # old function test
      # User.where(:salesforce_instance_url.ne => nil).pluck(:salesforce_instance_url).uniq.each do |server_url|
      #   instance = server_url.match(/https:\/\/[a-z]{2}[0-9]{1,2}/).to_s.gsub("https://","")
      #   instance = server_url.split(".salesforce.com")[0].split("://")[1] if instance.nil? || instance.empty?
      #   if instance.include? 'cloudforce.com'
      #     host = instance
      #   else
      #     host = "#{instance}.salesforce.com"
      #   end
      #   if "https://#{host}" != server_url
      #       puts "#{server_url} => #{host}"
      #   end
      # end
      #
      # old function result:
      # https://na130.salesforce.com => na13.salesforce.com
      # https://na112.salesforce.com => na11.salesforce.com
      # https://na132.salesforce.com => na13.salesforce.com
      # https://na174.salesforce.com => na17.salesforce.com
      # https://na100.salesforce.com => na10.salesforce.com
      # https://na124.salesforce.com => na12.salesforce.com
      # https://na131.salesforce.com => na13.salesforce.com
      # https://na136.salesforce.com => na13.salesforce.com
      # https://na129.salesforce.com => na12.salesforce.com
      # https://na134.salesforce.com => na13.salesforce.com
      # https://na111.salesforce.com => na11.salesforce.com
      # https://na102.salesforce.com => na10.salesforce.com
      # https://na172.salesforce.com => na17.salesforce.com
      # https://na115.salesforce.com => na11.salesforce.com
      # https://na116.salesforce.com => na11.salesforce.com
      # https://ns8.my.salesforce.com => ns8.salesforce.com
      # https://na171.salesforce.com => na17.salesforce.com
      # https://td123.my.salesforce.com => td12.salesforce.com
      # https://na114.salesforce.com => na11.salesforce.com
      # https://na135.salesforce.com => na13.salesforce.com
      # https://na173.salesforce.com => na17.salesforce.com
      # https://na101.salesforce.com => na10.salesforce.com
      # https://na119.salesforce.com => na11.salesforce.com
      # https://na122.salesforce.com => na12.salesforce.com
      # https://eb1234.my.salesforce.com => eb12.salesforce.com
      # https://na103.salesforce.com => na10.salesforce.com
      # https://na123.salesforce.com => na12.salesforce.com
      #
      # new function test
      # User.where(:salesforce_instance_url.ne => nil).pluck(:salesforce_instance_url).uniq.each do |server_url|
      #   instance = server_url.split(".salesforce.com")[0].split("://")[1]
      #   if instance.include? 'cloudforce.com'
      #     host = instance
      #   else
      #     host = "#{instance}.salesforce.com"
      #   end
      #   if "https://#{host}" != server_url
      #       puts "#{server_url} => #{host}"
      #   end
      # end; puts
      #
      # Original line deleted:
      # @instance = @server_url.match(/https:\/\/[a-z]{2}[0-9]{1,2}/).to_s.gsub("https://","")
      @instance = @server_url.split(".salesforce.com")[0].split("://")[1]
      return @instance
    end

    def counters
      {
        get: get_counters[:get],
        post: get_counters[:post]
      }
    end

    private

    def get_counters
      @counters ||= Hash.new(0)
    end

    def count(http_method)
      get_counters[http_method] += 1
    end

  end

end
