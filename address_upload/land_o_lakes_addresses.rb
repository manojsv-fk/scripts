require 'csv'
require 'json'
require 'optparse'
require 'io/console'
require 'uri'
require 'net/http'
require 'openssl'
require 'roo'

class BatchUpdate
  attr_accessor :username, :password, :input_file_name, :env, :company, :errors_file_name, :headers, :url, :errors , :invalid_addresses , :location_ids

  BATCH_SIZE = 3
  
  ROW_HEADING = [:address_id, :location_id, :ship_to, :stop_name, :address_line_1, :address_line_2, :city, :state, :zip, :country,
    :load_unload_time, :latitude, :longitude, :geofence_radius, :polygon_coordinates, :tags, :mon_open, :mon_close, :tues_open, :tues_close,
    :wed_open, :wed_close, :thurs_open, :thurs_close, :fri_open, :fri_close, :sat_open, :sat_close, :sun_open, :sun_close]
    
  def initialize
    print "Input file name:"
    self.input_file_name = gets.chomp
    abort if !validate_file self.input_file_name
    
    print "Email address:"
    self.username = gets.chomp
    
    print "Password:"
    self.password = STDIN.noecho(&:gets).chomp
    puts
    
    print "Environment (dev/local/prod/staging):"
    self.env = gets.chomp
    
    # abort if !validate_env self.env
    
    print "Company Permalink:"
    self.company = gets.chomp
    
    puts "Errors will be saved in ==> #{errors_file_name}"
    
    set_url
    self.headers = {}
    self.errors = ""
    self.invalid_addresses = []
    self.location_ids = []
  end

  def get_invalid_address
    self.invalid_addresses
  end

  def get_location_ids
    self.location_ids
  end

  def process_file
    extension = File.extname input_file_name
    
    if extension.include? "csv"
      process_csv_file 
    else
      process_xlsx_file
    end
  end
  
  def process_csv_file
    map_csv_headers
    
    payloads = []
    CSV.foreach(self.input_file_name, headers: true).with_index do |row, i|
      row_hash = JSON.parse(create_row_hash(row))
      p row_hash['locationId']
      p row_hash['unloadTimeInMinutes']
      puts "******************************"
      self.location_ids << row_hash['locationId'] if row_hash['unloadTimeInMinutes'] == 30
      payloads << row_hash if valid_address?(row_hash)
      self.invalid_addresses << row_hash unless valid_address?(row_hash)

      # if i % BATCH_SIZE == 0 and i > 1
      #   puts "processing row #{i}"
      #   batch_create payloads, i / BATCH_SIZE if payloads.present?
      #   payloads = []
      # end
    end

    # if !payloads.empty?
    #   batch_create payloads, "end"
    # end
    
    write_errors
  end

  def process_xlsx_file
    xls_file = Roo::Excelx.new(self.input_file_name)
    
    puts "Converting excel to csv file .."
    new_file_name = self.input_file_name.split(".")[0] + ".csv"
    xls_file.to_csv(filename = new_file_name)
    
    
    rows = []
    headers = []
    CSV.foreach(new_file_name).with_index do |row, i|
      
      if i == 0
        headers = row
        next
      end
      
      rows << row
    end
    
    CSV.open(new_file_name, "w", headers: true, write_headers: headers) do |writer|
      rows.each do |row|
        writer << row
      end
    end
    
    puts "File converted to csv.."
    self.input_file_name = new_file_name
    
    process_csv_file
  end
  
  private
  
  def validate_file(file)
    valid_extensions = [".csv", ".xlsx"]
    extension = File.extname file
    
    if !valid_extensions.include? extension
      puts "Only Excel and CSV files are permitted."
      return false
    end
    
    if !File.file?(file)
      puts "file does not exist in the system. Make sure the file exist in the system."
      return false
    end
    
    self.errors_file_name = file.split(".")[0] + "-errors" + ".txt"
    return true
  end
  
  def validate_env(env)
    if !["dev", "prod", "staging", "local"].include? env
      puts "environment name is wrong"
      return false
    end
    
    return true
  end
  
  def map_csv_headers
    csv_headers = []
    i = 0
    
    CSV.foreach(input_file_name) do |row|
      csv_headers = row
      i = i + 1
      break if i == 1
    end

    csv_headers.each do |col_name|
      next if col_name.blank?
      col = col_name.downcase

      if col.include? "location" and col.include? "id"
        self.headers[:location_id] = col_name
        next
      end
      
      if col == "name"
        self.headers[:stop_name] = col_name
        next
      end
      
      if col == "address"
        self.headers[:address_line_1] = col_name
        next
      end
      
      if col.include? "city"
        self.headers[:city] = col_name
        next
      end
      
      if col.include? "state"
        self.headers[:state] = col_name
        next
      end
      
      if col.include? "country"
        self.headers[:country] = col_name
        next
      end
      
      
      if col.include? "load" and col.include? "custom" and col.include? "time"
        self.headers[:load_unload_time] = col_name
        next
      end
        
      if col.include? "tags"
        self.headers[:tags] = col_name
        next
      end
    end
    
    puts self.headers
  end
  
  def create_row_hash(row)
    row_hash = {}
    row.each do |key, value|
      next if key.nil?
      row_hash[self.headers.key(key)] = value
    end
    row_hash.delete(:address_id) if row_hash[:address_id].nil?
    row_hash.delete(:location_id) if row_hash[:location_id].nil?
    get_json_hash(row_hash)
  end
  
  def get_json_hash(row_hash)
    unload_time_in_mins = row_hash[:load_unload_time].present? ? row_hash[:load_unload_time].to_i : 90
    json_hash = {"enabledCheckboxLabel" => "Business hours for this stop", "locationId" => row_hash[:location_id].to_s, "curatedAddressLine1" => row_hash[:address_line_1], 
      "curatedAddressLine2" => row_hash[:address_line_2], "curatedCity" => row_hash[:city], "curatedState" => row_hash[:state],
      "curatedPostal" => row_hash[:zip], "country" => row_hash[:country],
      "name" => row_hash[:stop_name].to_s, "unloadTimeInMinutes" => unload_time_in_mins}
    json_hash.to_json
  end

  def valid_address?(payload)
    (payload['curatedAddressLine1'].present? || (payload['curatedCity'].present? && payload['curatedState'].present? && payload['country'].present?) || (payload['latitude'].present? && 
      payload['longitude'].present?))
  end
  
  def batch_create(payloads, batch_num)
    return if batch_num > 2
    puts "Batch :: #{batch_num}"
    p payloads
    request_payload = {
      addresses: payloads
    }.to_json
    
    url = self.url + "/api/v1/address/create_or_update?company_id=#{self.company}"
    uri = URI.parse(url)
    http = Net::HTTP.new(uri.host, uri.port)
    
    if self.env != "local"
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    end
    
    begin
      request = Net::HTTP::Post.new(uri.request_uri)
      request.body = request_payload
      request["content-type"] = "application/json"
      request["cache-control"] = "no-cache"
      request.basic_auth(self.username, self.password)
      response = http.request(request)
      
      response_hash = JSON.parse(response.read_body)
    
      if response_hash["statusCode"] == 200
          puts "Success"
      else
          puts "Error occured"
          puts response_hash
          errors << "Batch number :: #{batch_num}, creation failed ::"
          errors << "Status code ==> #{response_hash['statusCode']}\n"
          
      end
    rescue Exception => e
      errors << "Exception while processing rows #{batch_num} :: #{e.message}"
      errors << "\n"
    ensure
      puts "sleep for 10 seconds"
      sleep 5
    end
  end
  
  def set_url
    case self.env
      when "dev"
        self.url = "https://geo-api-dev.fourkites.com"
      when "local"
        self.url = "http://localhost:3000"
      when "staging"
        self.url = "https://geo-api-staging.fourkites.com"
      when "prod"
        self.url = "https://geo-api.fourkites.com"
      when "uat"
        self.url = "https://geo-api-uat.fourkites.com"
      end
    end
    
  def write_errors
    File.write(self.errors_file_name, errors) if !errors.empty?
  end
end

start = Time.now
updater = BatchUpdate.new
/Users/manojsv/Desktop/trac_5887.csv
updater.process_file
updater.get_location_ids
finish = Time.now
puts "Took #{finish-start} seconds.."


