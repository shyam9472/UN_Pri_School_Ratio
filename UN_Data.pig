REGISTER piggybank.jar;


--Using csv the un data file is accessed and a ratio is generated from it.
data = LOAD '/user/hue/Data/UN_Primary.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (country:chararray , gender:chararray, year:chararray,source:chararray,unit:chararray,value:int,footnotes:int);

flt1 = filter data by gender == 'Male';

flt2 = filter data by gender == 'Female';

--dump flt1;

year = join flt1 by (country, year), flt2 by (country, year);

ratio = foreach year generate flt1::country, flt1::year, (float)flt1::value / (float)flt2::value;

dump ratio;


xmldata = load '/user/hue/UNdata_xml.xml' USING org.apache.pig.piggybank.storage.XMLLoader('record') as(doc:chararray);

data = foreach xmldata GENERATE FLATTEN(REGEX_EXTRACT_ALL(doc,'<record>\\s*<field name="Country or Area">(.*)</field>\\s*<field name="Subgroup">(.*)</field>\\s*<field name="Year">(.*)</field>\\s*<field name="Source">(.*)</field>\\s*<field name="Unit">(.*)</field>\\s*<field name="Value">(.*)</field>\\s*<field name="Value Footnotes">(.*)</field>\\s*</record>')) AS (country:chararray, subgroup:chararray, year:int, source:chararray, unit:chararray, value:chararray, value_footnotes:chararray);

dump data;
