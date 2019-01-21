require 'roo'
require 'json'
require 'uri'
require 'csv'

class DeleteDeactivatedUsers

  HEADING_HASH = {first: 'First', last: 'Last', email_address: 'Email Address'}

  def initialize(arguements)
    @input_file_name = arguements[0]
    @company_id = arguements[1]
    @output_file_name = arguements[2]
    @not_deleted = []
    @deleted_users = []
    @all_users_of_company  = User.where(company_id: @company_id)
  end

  def process_file
    if arguements_correct?
      create_output_file

      process_individual_records do |row_hash, row_num|
        populate_nil_values row_hash
        
        begin
          delete_user row_hash, row_num
        rescue => e
          puts "#{row_num}, #{e.message}"
          log_error_in_output_file row_num, "Exception happened when executing script. Error - #{e.backtrace}"
        end
      end
    else
      puts "Wrong arguements"
    end
    @all_users_of_company = []
  end

  def get_undeleted_users
    @not_deleted 
  end

  def get_deleted_users
    @deleted_users
  end

  private

  def arguements_correct?
    arguements_incorrect = @input_file_name.nil? || !([".xlsx", ".csv"].include?(File.extname(@input_file_name))) || !File.file?(@input_file_name)|| 
    @company_id.nil? || @output_file_name.nil? || !File.directory?(File.dirname(@output_file_name))

    !arguements_incorrect
  end


  def process_individual_records
    file_extn = File.extname(@input_file_name)
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
        puts row_hash
        yield(row_hash, row_num)
      end
    elsif file_extn == ".csv"
      CSV.foreach(@input_file_name, headers: true).with_index do |row , row_num|
        row_num += 2
        row_hash = get_row_hash(row)
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

 def delete_user(user_hash, row_number)
  puts "Row #{row_number} in progress "
  p user_hash
  accounts_with_specified_email = @all_users_of_company.where(email_address: user_hash[:email_address])#, active: false)
  puts "More than one account with email #{user_hash[:email_address]}" if accounts_with_specified_email.count > 1
  user_to_delete = accounts_with_specified_email.first
  user_to_delete.delete if user_to_delete.present?
  puts "Not deleted #{user_hash[:email_address]}" unless user_to_delete.present?
  @not_deleted << user_hash[:email_address] unless user_to_delete.present?

  puts "Deleted user #{user_to_delete.email_address}" if user_to_delete.present?
  @deleted_users << user_to_delete.email_address if user_to_delete.present?
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

users_deleter = DeleteDeactivatedUsers.new( ["/home/ec2-user/manojsv/c-and-s-users-trac-6110.xlsx", 'c-and-s-wholesale', "/home/ec2-user/manojsv/trac-6110_output.csv"])
users_deleter = DeleteDeactivatedUsers.new( ["/Users/manojsv/Desktop/c-and-s-users.xlsx", 'manojcarrier', "/Users/manojsv/Documents/run1_output.csv"])
users_deleter.process_file

users_deleter.get_undeleted_users

users_deleter.get_deleted_users
