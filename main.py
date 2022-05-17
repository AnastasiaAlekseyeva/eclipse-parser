# -*- coding: utf-8 -*-

from lib import schedule_parser

if __name__ == "__main__":
	keywords = ("DATES", "COMPDAT", "COMPDATL")
	parameters = ("Date", "Well name", "Local grid name", "I", "J", "K upper", "K lower", "Flag on connection",
					"Saturation table", "Transmissibility factor", "Well bore diameter", "Effective Kh",
					"Skin factor", "D-factor", 'Dir_well_penetrates_grid_block', 'Press_eq_radius')
	input_file = 'input_data/test_schedule.inc'
	clean_file = "output_data/handled_schedule.inc"
	output_csv = "output_data/schedule.csv"

	schedule_df = schedule_parser.transform_schedule(keywords, parameters, input_file, output_csv, clean_file)
