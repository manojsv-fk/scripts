require 'csv'
require 'json'
require 'optparse'
require 'io/console'
require 'uri'
require 'net/http'
require 'openssl'
require 'roo'

class BatchUpdate
  attr_accessor :username, :password, :input_file_name, :env, :company, :errors_file_name, :headers, :url, :errors , :invalid_addresses

  BATCH_SIZE = 100
  
  ROW_HEADING = [:address_id, :location_id, :ship_to, :stop_name, :address_line_1, :address_line_2, :city, :state, :zip, :country,
    :load_unload_time, :latitude, :longitude, :geofence_radius, :polygon_coordinates, :tags, :mon_open, :mon_close, :tues_open, :tues_close,
    :wed_open, :wed_close, :thurs_open, :thurs_close, :fri_open, :fri_close, :sat_open, :sat_close, :sun_open, :sun_close, :timezone]
    
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
    
    abort if !validate_env self.env
    
    print "Company Permalink:"
    self.company = gets.chomp
    
    puts "Errors will be saved in ==> #{errors_file_name}"
    
    set_url
    self.headers = {}
    self.errors = ""
    self.invalid_addresses = []
  end

  def get_invalid_address
    self.invalid_addresses
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
      payloads << row_hash if valid_address?(row_hash)
      self.invalid_addresses << row_hash unless valid_address?(row_hash)

      if i % BATCH_SIZE == 0 and i > 1
        puts "processing row #{i}"
        batch_create payloads, i / BATCH_SIZE 
        payloads = []
      end
    end

    if !payloads.empty?
      batch_create payloads, "end"
    end
    
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
      
      if col == "id"
        self.headers[:address_id] = col_name
        next
      end
      
      if col.include? "location" and col.include? "id"
        self.headers[:location_id] = col_name
        next
      end
      
      if col.include? "ship" and col.include? "to"
        self.headers[:ship_to] = col_name
        next
      end
      
      if col.include? "stop" and col.include? "name"
        self.headers[:stop_name] = col_name
        next
      end
      
      if col.include? "address" and col.include? "line" and col.include? "1"
        self.headers[:address_line_1] = col_name
        next
      end
      
      if col.include? "address" and col.include? "line" and col.include? "2"
        self.headers[:address_line_2] = col_name
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
      
      if col.include? "zip" or col.include? "postal"
        self.headers[:zip] = col_name
        next
      end
      
      if col.include? "country"
        self.headers[:country] = col_name
        next
      end
      
      
      if col.include? "load" and col.include? "un" and col.include? "time"
        self.headers[:load_unload_time] = col_name
        next
      end
      
      if col.include? "lat" and !(col.include? "pair" or col.include? "point")
        self.headers[:latitude] = col_name
        next
      end
      
      if col.include? "long" and !(col.include? "pair" or col.include? "point")
        self.headers[:longitude] = col_name
        next
      end
      
      if col.include? "rad" and col.include? "miles"
        self.headers[:geofence_radius] = col_name
        next
      end
      
      if (col.include? "polygon" or col.include? "point" or col.include? "array" or col.include? "pair" or col.include? "polygon") and !col.include? "tag"
        self.headers[:geofence_points] = col_name
        next
      end
        
      if col.include? "tags"
        self.headers[:tags] = col_name
        next
      end
      
      if col.include? "mon" and col.include? "open"
        self.headers[:mon_open] = col_name
        next
      end
      
      if col.include? "mon" and col.include? "close"
        self.headers[:mon_close] = col_name
        next
      end
      
      if col.include? "tue" and col.include? "open"
        self.headers[:tues_open] = col_name
        next
      end
      
      if col.include? "tue" and col.include? "close"
        self.headers[:tues_close] = col_name
        next
      end
      
      if col.include? "wed" and col.include? "open"
        self.headers[:wed_open] = col_name
        next
      end
      
      if col.include? "wed" and col.include? "close"
        self.headers[:wed_close] = col_name
        next
      end
      
      if col.include? "thur" and col.include? "open"
        self.headers[:thurs_open] = col_name
        next
      end
      
      if col.include? "thur" and col.include? "close"
        self.headers[:thurs_close] = col_name
        next
      end
      
      if col.include? "fri" and col.include? "open"
        self.headers[:fri_open] = col_name
        next
      end
      
      if col.include? "fri" and col.include? "close"
        self.headers[:fri_close] = col_name
        next
      end
      
      if col.include? "sat" and col.include? "open"
        self.headers[:sat_open] = col_name
        next
      end
      
      if col.include? "sat" and col.include? "close"
        self.headers[:sat_close] = col_name
        next
      end
      
      if col.include? "sun" and col.include? "open"
        self.headers[:sun_open] = col_name
        next
      end
      
      if col.include? "sun" and col.include? "close"
        self.headers[:sun_close] = col_name
        next
      end

      if col.include? "time" and col.include? "zone"
        self.headers[:timezone] = col_name
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
    geofence_points = get_geofence_points(row_hash)
    geofence_radius = get_geofence_radius(row_hash)
    cut_off_time_array, cut_off_time_enabled = get_cut_off_times(row_hash)
    tags = get_tags(row_hash)
    
    json_hash = {"enabledCheckboxLabel" => "Business hours for this stop", "locationId" => row_hash[:location_id].to_s, "curatedAddressLine1" => row_hash[:address_line_1], 
      "curatedAddressLine2" => row_hash[:address_line_2], "curatedCity" => row_hash[:city], "curatedState" => row_hash[:state],
      "curatedPostal" => row_hash[:zip], "country" => row_hash[:country], "latitude" => row_hash[:latitude], "longitude" => row_hash[:longitude],
      "name" => row_hash[:stop_name], "unloadTimeInMinutes" => row_hash[:load_unload_time], "tags" => tags, "isCutoffTimeEnabled" => true, "reverseGeocode" => false,
      "geofencePoints" => geofence_points, "geofenceRadius" => geofence_radius, "cutoffTimes" => [], "timezone" => row_hash[:timezone]}
    json_hash["cutoffTimes"] = cut_off_time_array if cut_off_time_enabled


    json_hash.to_json
  end

  def valid_address?(payload)
    (payload['curatedAddressLine1'].present? || (payload['curatedCity'].present? && payload['curatedState'].present? && payload['country'].present?) || (payload['latitude'].present? && 
      payload['longitude'].present?)) && payload['locationId'] != 'V50025505'
  end
  
  def batch_create(payloads, batch_num)
    puts "Batch :: #{batch_num}"
    p payloads
    request_payload = {
      addresses: payloads
    }.to_json
    
    url = self.url + "/api/v1/address/batch_create?company_id=#{self.company}"
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
      sleep 10
    end
  end
  
  # [(x1,y1),(x2,y2)]
  # [{"latitude": x1, "longitude": y1}, {"latitude": x2, "longitude": y2}]
  def get_geofence_points(row_hash)
    geofence_points = row_hash[:geofence_points]
    unless geofence_points.nil?
      coordinates_array = geofence_points.split(",")
      geofence_points = []
      coordinates_array.each_slice(2) { |lat , long| 
        lati = lat.present? ? lat.gsub(/[^0-9.-]/i, '') : nil
        longi = long.present? ? long.gsub(/[^0-9.-]/i, '') : nil
        geofence_points << {latitude: lati, longitude: longi} 
      } 
    end
    geofence_points
  end
  
  def get_geofence_radius(row_hash)
    return nil if row_hash[:geofence_radius].nil?
    
    convert_miles_to_meter(row_hash[:geofence_radius].to_f)
  end
  
  def get_cut_off_times(address_hash)
    cut_off_times = []
    is_cut_off_time_enabled = false
    open_close_days_arr = [[:sun_open, :sun_close], [:mon_open, :mon_close], [:tues_open, :tues_close], [:wed_open, :wed_close], 
      [:thurs_open, :thurs_close], [:fri_open, :fri_close], [:sat_open , :sat_close]]
    cut_off_times = []
    0.upto(6) do |day_num|
      start_cut_off_time = address_hash[open_close_days_arr[day_num][0]]
      end_cut_off_time = address_hash[open_close_days_arr[day_num][1]]
      
      if start_cut_off_time.nil? or end_cut_off_time.nil?
        next
      end
      
      is_cut_off_time_enabled ||= !(start_cut_off_time.empty? && end_cut_off_time.empty?)
      start_cut_off_time = "00:00" if start_cut_off_time.empty?
      end_cut_off_time = "23:59" if end_cut_off_time.empty?
      is_working_day = is_working_day?(start_cut_off_time, end_cut_off_time)
      cut_off_times << {dayEnum: day_num, startCutoffTime: get_proper_time_format(start_cut_off_time), 
        endCutoffTime: get_proper_time_format(end_cut_off_time), isWorkingDay: is_working_day}.to_hash
    end
    [cut_off_times, is_cut_off_time_enabled]
  end
  
  def is_working_day?(startCutoffTime , endCutoffTime)
    !((startCutoffTime == "00:00" && endCutoffTime == "00:00") || (startCutoffTime == "0:00" && endCutoffTime == "0:00"))
  end
  
  def get_proper_time_format(time)
    if time.length == 4
      time = "0#{time}"
    end
    time
  end
  
  def get_tags(address_hash)
    tags = address_hash[:tags]
    return "" if tags.nil? or tags.empty?
    
    if(tags!= "")  
      tags = address_hash[:tags].split(/\s*,\s*/) #split by comma with leading and trailing spaces. 
    end
    tags
  end
  
  def convert_miles_to_meter(miles)
    (miles * 1.609344 * 1000).round(4)
  end
  
  def set_url
    case self.env
      when "dev"
        self.url = "https://geo-api-dev.fourkites.com"
      when "local"
        self.url = "http://localhost:3001"
      when "staging"
        self.url = "https://geo-api-staging.fourkites.com"
      when "prod"
        self.url = "https://geo-api.fourkites.com"
      end
    end
    
  def write_errors
    File.write(self.errors_file_name, errors) if !errors.empty?
  end
end

start = Time.now
updater = BatchUpdate.new
updater.process_file
finish = Time.now
puts "Took #{finish-start} seconds.."