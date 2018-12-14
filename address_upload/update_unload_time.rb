require 'roo'
require 'json'
require 'uri'
require 'csv'

class UpdateUnloadTime

  HEADING_HASH = {location_id: 'Location ID', tags: 'Domain', unload_time: 'Custom Load/Unload time'}

  def initialize(arguements)
    @input_file_name = arguements[0]
    @all_addresses  = arguements[1]
    @updated_addresses = []
    @not_updated = []
  end

  def process_file
    if arguements_correct?

      process_individual_records do |row_hash, row_num|
        
        begin
          update_address(row_hash, row_num)
        rescue => e
          puts "#{row_num}, #{e.message}"
        end
      end
    else
      puts "Wrong arguements"
    end
  end

  def get_not_updated_addresses
    @not_updated 
  end

  def get_updated_addresses
    @updated_addresses
  end

  private

  def update_address(addr_hash, row_num)
    begin
      puts "Going to update address no #{row_num}"
      p addr_hash
      location_id = addr_hash[:location_id]
      address = @all_addresses.where(location_id: location_id).first
      if address.blank?
        @not_updated << location_id
        return
      end
      tags = addr_hash[:tags]
      unload_time = addr_hash[:unload_time]
      address.update(tags: tags ,unload_time_in_minutes_provided: unload_time )
      @updated_addresses << location_id
    rescue Exception => e
      puts "Exception occured while updating address #{addr_hash[:location_id]}"
      p e.message
    end
  end

  def arguements_correct?
    arguements_incorrect = @input_file_name.nil? || !([".xlsx", ".csv"].include?(File.extname(@input_file_name))) || !File.file?(@input_file_name)

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
      row_heading = heading.include?('Domain') ? :tags : nil if row_heading.nil? 
      unless row_heading.nil?
        row_hash[row_heading] = value unless row_heading == :tags
        row_hash[row_heading] = [value] if row_heading == :tags
      end  
    end
    row_hash
  end

end

all_addresses = Address.where(company_id: 'lake-olakes')
#address_updater = UpdateUnloadTime.new( ["/Users/manojsv/Desktop/trac_5887.csv", all_addresses ])
address_updater = UpdateUnloadTime.new( ["/home/ec2-user/manojsv/trac_5887.csv", all_addresses ])
address_updater.process_file
address_updater.get_updated_addresses



