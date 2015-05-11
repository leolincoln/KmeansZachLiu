input_file = LOAD 'Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt' USING PigStorage('\t') AS (npi:chararray, nppes_provider_last:chararray, nppes_provider_first:chararray, nppes_provider_mi:chararray, nppes_credentials:chararray, nppes_provider_gender:chararray, nppes_entity_code:chararray, nppes_provider_street1:chararray, nppes_provider_street2:chararray, nppes_provider_city:chararray, nppes_provider_zip:chararray, nppes_provider_state:chararray, nppes_provider_country:chararray, provider_type:chararray, medicare_participation_indicator:chararray, place_of_service:chararray, hcpcs_code:chararray, hcpcs_description:chararray, line_srvc_cnt:double, bene_unique_cnt:double, bene_day_srvc_cnt:double, average_medicare_allowed_amt:double, stdev_medicare_allowed_amt:double, average_submitted_chrg_amt:double, stdev_submitted_chrg_amt:double, average_medicare_payment_amt:double, stdev_medicare_payment_amt:double);
ranked = RANK input_file;
NoHeader = FILTER ranked BY (rank_input_file > 2);
New_input_file = FOREACH NoHeader GENERATE 'same' AS key, line_srvc_cnt, bene_unique_cnt, bene_day_srvc_cnt, average_medicare_allowed_amt, stdev_medicare_allowed_amt, average_submitted_chrg_amt, stdev_submitted_chrg_amt, average_medicare_payment_amt, stdev_medicare_payment_amt;

grouped = GROUP New_input_file ALL;
maxes = FOREACH grouped GENERATE 'same' AS key, MAX(New_input_file.line_srvc_cnt) AS max_line_srvc_cnt, MAX(New_input_file.bene_unique_cnt) AS max_bene_unique_cnt, MAX(New_input_file.bene_day_srvc_cnt) AS max_bene_day_srvc_cnt, MAX(New_input_file.average_medicare_allowed_amt) AS max_average_medicare_allowed_amt, MAX(New_input_file.stdev_medicare_allowed_amt) AS max_stdev_medicare_allowed_amt, MAX(New_input_file.average_submitted_chrg_amt) AS max_average_submitted_chrg_amt, MAX(New_input_file.stdev_submitted_chrg_amt) AS max_stdev_submitted_chrg_amt, MAX(New_input_file.average_medicare_payment_amt) AS max_average_medicare_payment_amt, MAX(New_input_file.stdev_medicare_payment_amt) AS max_stdev_medicare_payment_amt;
mins = FOREACH grouped GENERATE 'same' AS key, MIN(New_input_file.line_srvc_cnt) AS min_line_srvc_cnt, MIN(New_input_file.bene_unique_cnt) AS min_bene_unique_cnt, MIN(New_input_file.bene_day_srvc_cnt) AS min_bene_day_srvc_cnt, MIN(New_input_file.average_medicare_allowed_amt) AS min_average_medicare_allowed_amt, MIN(New_input_file.stdev_medicare_allowed_amt) AS min_stdev_medicare_allowed_amt, MIN(New_input_file.average_submitted_chrg_amt) AS min_average_submitted_chrg_amt, MIN(New_input_file.stdev_submitted_chrg_amt) AS min_stdev_submitted_chrg_amt, MIN(New_input_file.average_medicare_payment_amt) AS min_average_medicare_payment_amt, MIN(New_input_file.stdev_medicare_payment_amt) AS min_stdev_medicare_payment_amt;

mins_maxes = JOIN mins BY key, maxes BY key;
max_minus_mins = FOREACH mins_maxes GENERATE 'same' AS key, max_line_srvc_cnt - min_line_srvc_cnt AS dif_line_srvc_cnt, max_bene_unique_cnt - min_bene_unique_cnt AS dif_bene_unique_cnt, max_bene_day_srvc_cnt - min_bene_day_srvc_cnt AS dif_bene_day_srvc_cnt, max_average_medicare_allowed_amt - min_average_medicare_allowed_amt AS dif_average_medicare_allowed_amt, max_stdev_medicare_allowed_amt - min_stdev_medicare_allowed_amt AS dif_stdev_medicare_allowed_amt, max_average_submitted_chrg_amt - min_average_submitted_chrg_amt AS dif_average_submitted_chrg_amt, max_stdev_submitted_chrg_amt - min_stdev_submitted_chrg_amt AS dif_stdev_submitted_chrg_amt, max_average_medicare_payment_amt - min_average_medicare_payment_amt AS dif_average_medicare_payment_amt, max_stdev_medicare_payment_amt - min_stdev_medicare_payment_amt AS dif_stdev_medicare_payment_amt;

input_mins = JOIN New_input_file BY key, mins BY key;
scaled_numerator = FOREACH input_mins GENERATE 'same' AS key, line_srvc_cnt - min_line_srvc_cnt AS x_line_srvc_cnt, bene_unique_cnt - min_bene_unique_cnt AS x_bene_unique_cnt, bene_day_srvc_cnt - min_bene_day_srvc_cnt AS x_bene_day_srvc_cnt, average_medicare_allowed_amt - min_average_medicare_allowed_amt AS x_average_medicare_allowed_amt, stdev_medicare_allowed_amt - min_stdev_medicare_allowed_amt AS x_stdev_medicare_allowed_amt, average_submitted_chrg_amt - min_average_submitted_chrg_amt AS x_average_submitted_chrg_amt, stdev_submitted_chrg_amt - min_stdev_submitted_chrg_amt AS x_stdev_submitted_chrg_amt, average_medicare_payment_amt - min_average_medicare_payment_amt AS x_average_medicare_payment_amt, stdev_medicare_payment_amt - min_stdev_medicare_payment_amt AS x_stdev_medicare_payment_amt;

num_den = JOIN scaled_numerator BY key, max_minus_mins BY key;
scaled_final = FOREACH num_den GENERATE x_line_srvc_cnt / dif_line_srvc_cnt AS scaled_line_srvc_cnt, x_bene_unique_cnt / dif_bene_unique_cnt AS scaled_bene_unique_cnt, x_bene_day_srvc_cnt / dif_bene_day_srvc_cnt AS scaled_bene_day_srvc_cnt, x_average_medicare_allowed_amt / dif_average_medicare_allowed_amt AS scaled_average_medicare_allowed_amt, x_stdev_medicare_allowed_amt / dif_stdev_medicare_allowed_amt AS scaled_stdev_medicare_allowed_amt, x_average_submitted_chrg_amt / dif_average_submitted_chrg_amt AS scaled_average_submitted_chrg_amt, x_stdev_submitted_chrg_amt / dif_stdev_submitted_chrg_amt AS scaled_stdev_submitted_chrg_amt, x_average_medicare_payment_amt / dif_average_medicare_payment_amt AS scaled_average_medicare_payment_amt, x_stdev_medicare_payment_amt / dif_stdev_medicare_payment_amt AS scaled_stdev_medicare_payment_amt;

scaled_hypothesis_B = FOREACH scaled_final GENERATE scaled_line_srvc_cnt, scaled_bene_day_srvc_cnt;
STORE scaled_hypothesis_B INTO '_B';
