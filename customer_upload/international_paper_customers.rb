require 'roo'
require 'json'
require 'uri'

class CustomerUploader
  include CustomerMapper

  HEADING_HASH = {customer_id: 'IDs', customer_name: 'Customer Name', priority: 'Priority'}
  

  def initialize(arguements)
    @input_file_name = arguements[0]
    @company_id = arguements[1]
    @output_file_name = arguements[2]
    @cust_array = []
    @cust_hash = {}
    @countt = 0
    @existing_customers = Company.find(@company_id).customers
    @existing_names = @existing_customers.map{|cus| cus.name}
  end

  def process_and_upload
    if arguements_correct?
      puts "Customers upload started successfully . Error results will be saved in #{@output_file_name} after this process complete"
      create_output_file

      process_individual_records do |row_hash, row_num|
        populate_nil_values row_hash
        puts "Row #{row_num} in progress "
        @countt = row_num
        # return if row_num >200
        begin
          row_hash[:customer_name] = row_hash[:customer_name].strip
          create_customer row_hash, row_num
        rescue => e
          puts "#{row_num}, #{e.message}"
          log_error_in_output_file row_num, "Exception happened when executing script. Error - #{e.backtrace}"
        end
      end
    else
      puts "wrong arguements passed. see below for correct command"
      puts "ruby bulk_customer_upload.rb <input_file_name (xlsx)> <company_id> <environment(/dev/staging/prod/uat)> <user_name> <password> <output csv file name>"
      puts "eg: ruby scripts/bulk_customer_upload.rb /Users/username/Desktop/customer_upload_3.xlsx test-shipper prod xxxx@fourkites.com xxxxxx /Users/user/outputs/run1_output.csv"
      puts "check wheher excel file and output file path are correct"
    end
  end

  def get_array
    @cust_array
  end

  def upload_all_customers
    puts "Count of elements"
    puts @countt
    puts @cust_array.count

    @cust_array.each_with_index do |customer, index|
      if @existing_names.exclude?(customer)
        upload_customer(customer, index)
      else
        update_customer(customer)
      end
    end
  end

  private

  def arguements_correct?
    arguements_incorrect = @input_file_name.nil? || !([".xlsx", ".csv"].include?(File.extname(@input_file_name))) || !File.file?(@input_file_name)|| 
    @company_id.nil? || #@env.nil? || !(['staging' , 'prod' , 'dev' , 'uat'].include? @env) || @user_name.nil? || @password.nil? || 
    @output_file_name.nil? || !File.directory?(File.dirname(@output_file_name))

    !arguements_incorrect
  end

  def process_individual_records
    file_extn = File.extname(@input_file_name)
    puts file_extn
    if file_extn == ".xlsx"
      spread_sheet = Roo::Spreadsheet.open(@input_file_name)
      row_info = {}
      spread_sheet.each_with_index do |row, row_num|
        # row_hash = Hash[*(HEADING_HASH.keys.zip(row).flatten)]
        row_num += 1
        if row_num == 1
          row_info = get_row_info(row)
          next
        else
          row_hash = get_row_hash_for_xslx_row(row, row_info)
        end
        #puts row_hash
        yield(row_hash, row_num)
      end
    elsif file_extn == ".csv"
      CSV.foreach(@input_file_name, headers: true).with_index do |row , row_num|
        row_num += 2
        row_hash = get_row_hash(row)
        puts "row hash #{row_hash}"
        yield(row_hash, row_num)
      end
    end
  end


  def get_row_hash_for_xslx_row(row, row_info)
    row_hash = {}
    row.each_with_index{|col, index|
      row_hash[row_info[index]] = col 
    }
    row_hash
  end

  def get_row_info(row)
    row_info = {}
    row.each_with_index{ |heading, index|
      row_info[index] = HEADING_HASH.key(heading) 
    }
    row_info
  end

  def get_row_hash(row)
    row_hash = {}
    row.each do |heading , value|
      row_heading = HEADING_HASH.key(heading)
      row_heading = :customer_name if (row_heading.nil? && heading.include?('Customer') && heading.include?('Name'))
      unless row_heading.nil?
        row_hash[row_heading] = value  
      end  
    end
    row_hash
  end

  def populate_nil_values(row_hash)
    row_hash.each do |key, array|
      row_hash[key] = "" if row_hash[key] == nil
    end
  end

   def create_customer(customer_hash, row_number)
    # add all unique customers to the array
    @cust_array << customer_hash[:customer_name] if @cust_array.exclude?(customer_hash[:customer_name])
    @cust_hash[customer_hash[:customer_name]] = [] if @cust_hash[customer_hash[:customer_name]].blank?
    @cust_hash[customer_hash[:customer_name]] << customer_hash[:customer_id].to_s
  end

  def upload_customer(customer_name, index)
    begin
      identifiers = @cust_hash[customer_name]
      customer_hash = get_request_payload_json({name: customer_name})
      params = map_customer_params(customer_hash.with_indifferent_access,'script-manoj')
      puts "Customer to be uploaded"
      puts params
      customer = Company::Customer.new(params)
      customer.company_id = @company_id
      customer.save!
      bulk_insert_identifiers(customer.id , identifiers) if identifiers.present? 
    rescue Exception => e
      puts "Customer not created #{e.message}"
    end
    
  end

  def update_customer(customer_name)
    begin
      puts "updating customer #{customer_name}"
      update_params = {}
      customer_identifier_ids = []
      customer = @existing_customers.where(name: customer_name).first
      identifiers = @cust_hash[customer_name]
      identifiers.each do |id|
         customer_identifier_ids << {identifier: prepare_customer_id(id)}
      end
      update_params[:identifiers_attributes] = customer_identifier_ids
      customer.update(update_params)
      
    rescue Exception => e
      puts "Error in updating #{e.message}"
    end
  end

  def prepare_customer_id(customer_id)
    id = customer_id.to_s.strip.downcase
    id =~ /\A\d*\z/ ? id.gsub(/\A0*/, '') : id
  end

  def get_bulk_insert_query(customer_id, identifiers)
    time_now = Time.now.utc.to_s
    puts "#{identifiers.count} identifiers for customer #{customer_id}"
    sql_insert_string = "INSERT INTO company_customer_identifiers (identifier, customer_id, created_at, updated_at) VALUES "
    values_string = ""
    identifiers.each do |identifier|
      values_string += "('#{identifier.to_s.strip}', #{customer_id}, '#{time_now}', '#{time_now}'),"
    end
    total_query = sql_insert_string + values_string[0..-2]
    puts total_query
    total_query
  end

  def bulk_insert_identifiers(customer_id, identifiers)
    insert_query = get_bulk_insert_query(customer_id, identifiers)
    begin
      ActiveRecord::Base.connection.execute(insert_query)
    rescue Exception => e
      puts "Error occured : #{e.message}"
    end
  end

  def get_comma_sepereated_ids(ids)
    ids = ids.to_s
    id_arr = []
    id_arr = ids.split(",") if !ids.nil?
    id_arr
  end

  def get_request_payload_json(customer_hash)
    # json_hash = {"name" => customer_name , "identifiers" => @cust_identifier_hash[customer_name]}
    priority = customer_hash[:priority].downcase if customer_hash[:priority].present?
    json_hash = {"name" => customer_hash[:name] , "identifiers" => [], "active" => true, "priority" => priority}
    json_hash
  end

  def create_output_file
    CSV.open(@output_file_name, "wb") do |csv|
      csv << ["Row Number", "Status", "Error"]
    end
  end

  def log_error_in_output_file(current_row_num, error_msg)
    CSV.open(@output_file_name, "ab") do |csv|
      csv << [current_row_num, "failure", error_msg]
    end
  end

end

# customer_uploader = CustomerUploader.new ARGV
# customer_uploader.process_and_upload
# Company::Customer.where(company_id: company.id).update_all(deleted: true)
company = Company.find_by_permalink('international_paper')
customer_uploader = CustomerUploader.new( ["/Users/manojsv/Desktop/international_paper_customers.csv", company.id, "/Users/manojsv/Desktop/output.csv"])
customer_uploader = CustomerUploader.new( ["/home/ec2-user/manojsv/international_paper_customers.csv", company.id, "/home/ec2-user/manojsv/international_paper_output.csv"])

#customer_uploader = CustomerUploader.new( ["/Users/manojsv/Desktop/ffe_new.xlsx", company.id, "/Users/manojsv/Desktop/run1_output.csv"])

customer_uploader.process_and_upload

customer_uploader.upload_all_customers

