import itertools 
from itertools import permutations
import pandas as pd
import numpy as np

from name_check import email_fill,name_check

column_names = ["Attendee_ID", "Name", "Position","BHCMISID","Email"]


# function that reads in two zoom csv's and the main attendance dataframe, checks to see if a user from one of the zoom csv's doesn't exist
# in the main attendance dataframe, if doesn't exist, adds the user to the dataframe and assigns a unique id
# returns main dataframe
def drvs_database(database_df_path, registration_drvs_df_path, attendance_drvs_df_path):

	# read in dataframe and csv's
	database_df = pd.read_excel(database_df_path, sheet_name = 0)
	database_df = database_df[["Attendee_ID", "Name", "Position","BHCMISID","Email"]]
	registration_drvs = pd.read_csv(registration_drvs_df_path)
	attendance_drvs = pd.read_csv(attendance_drvs_df_path, skiprows = 2)

	#in registration df, combines first and last name into full name column
	registration_drvs['Name'] = registration_drvs['First Name'].str.cat(registration_drvs['Last Name'],sep=" ")

	#drop all nan values in email columns
	registration_drvs = registration_drvs.dropna(subset=['Email'])
	attendance_drvs = attendance_drvs.dropna(subset=['User Email'])

	#add BHCMISID to registration df
	#adds center BHCIMISID to attendance df 
	registration_drvs = name_check(registration_drvs,'Organization')
	attendance_drvs = email_fill(attendance_drvs, 'User Email')
	attendance_drvs = name_check(attendance_drvs,'Center')
	
	attendance_drvs["Job Title"] = np.nan

	#iterate through emails in registration df
	# if email not in main attendance dataframe, add to main attendance dataframe
	for index, row in registration_drvs.iterrows():
		if row["Email"] not in database_df["Email"].values and "lpca" not in row["Email"]:
			database_df.loc[len(database_df.index)] = [(len(database_df.index) + 1), row["Name"], row["Job Title"],row["BHCMISID"],row["Email"]]
	
	#iterate through emails in attendance df
	# if email not in main attendance dataframe, add to main attendance dataframe
	for index, row in attendance_drvs.iterrows():
		if row["User Email"] not in database_df["Email"].values and "lpca" not in row["User Email"]:
			print(len(database_df.index))
			database_df.loc[len(database_df.index)] = [(len(database_df.index) + 1), row["Name (Original Name)"],row["Job Title"],row["BHCMISID"],row["User Email"]]

	return(database_df)

def meeting_attendance(database_df_path, meeting_atttendance_df_path, registration_drvs_df_path,attendance_drvs_df_path):

	#read in main dataframe
	main_df = drvs_database(database_df_path,registration_drvs_df_path,attendance_drvs_df_path)
	#read in meeting_attendance df
	meeting_attendance = pd.read_excel(meeting_atttendance_df_path, sheet_name=1)
	#get subset of meeting_attendance df with only necessary columns
	meeting_attendance = meeting_attendance[["Attendance_ID", "Meeting_ID", "Attendee_ID","Duration"]]
	
	# read in attendance drvs zoom csv
	attendance_drvs = pd.read_csv(attendance_drvs_df_path,skiprows = 2)
	# drop any rows with empty emails
	attendance_drvs = attendance_drvs.dropna(subset=['User Email'])
	# fill in center column with email
	attendance_drvs = email_fill(attendance_drvs, 'User Email')
	# fill in BHCMISID using center column
	attendance_drvs = name_check(attendance_drvs,'Center')
	
	#create variable for current meeting_idf
	#will be 1+ previous meeting id
	meeting_id = meeting_attendance['Meeting_ID'].iloc[-1] + 1

	#iterate through columns and select attendee id's matched to emails in attendance drvs df
	# get attendee id's of all users who attended current drvs user group
	for index, row in attendance_drvs.iterrows():
		attendee_id = (main_df.loc[main_df['Email'] == row["User Email"]])['Attendee_ID']
		main_df.loc[main_df['Email'] == row["User Email"]]

		# set meeting attendance id to equal last indix plus one
		# meeting_id is set to variable set earlier
		# attendee id is found by matching id to email
		# duration found by iterating through attendance drvs df and adding it to meeting_attendance
		meeting_attendance.loc[len(meeting_attendance.index)] = [(len(meeting_attendance.index) + 1), meeting_id, attendee_id, row["Duration (Minutes)"]]
	return meeting_attendance
 
def meeting_id(database_df_path,attendance_drvs,cqm_focus_Yes_or_No):
	meeting_id_df = pd.read_excel(database_df_path, sheet_name=2)
	attendance_drvs_df = pd.read_csv(attendance_drvs)
	meeting_id_df.loc[len(meeting_id_df.index)] = [(len(meeting_id_df.index) + 1), pd.to_datetime(attendance_drvs_df['Start Time'].iloc[0]), attendance_drvs_df['Topic'].iloc[0],cqm_focus_Yes_or_No]
	return meeting_id_df


#function that uses the other functions together to update the main attendance drvs database and all its sheets
def update_attendance_drvs(database_df_path, registration_drvs_df_path,attendance_drvs_df_path,cqm_focus_Yes_or_No):
		# get main df and new attendance drvs files
		attendees_df = drvs_database(database_df_path,registration_drvs_df_path,attendance_drvs_df_path)
		meeting_attendance = pd.read_excel(database_df_path, sheet_name=1)
		meeting_attendance = meeting_attendance[["Attendance_ID", "Meeting_ID", "Attendee_ID","Duration"]]

		# read in attendance drvs zoom csv
		attendance_drvs = pd.read_csv(attendance_drvs_df_path,skiprows = 2)
		# drop any rows with empty emails
		attendance_drvs = attendance_drvs.dropna(subset=['User Email'])
		# fill in center column with email
		attendance_drvs = email_fill(attendance_drvs, 'User Email')
		# fill in BHCMISID using center column
		attendance_drvs = name_check(attendance_drvs,'Center')

		#create variable for current meeting_idf
		#will be 1+ previous meeting id
		meeting_ids = meeting_attendance['Meeting_ID'].iloc[-1] + 1

		#iterate through columns and select attendee id's matched to emails in attendance drvs df
		# get attendee id's of all users who attended current drvs user group
		for index, row in attendance_drvs.iterrows():
			attendee_id = (attendees_df.loc[attendees_df['Email'] == row["User Email"]])['Attendee_ID']
			if attendee_id.empty == True:
				pass
			else:
				attendees_df.loc[attendees_df['Email'] == row["User Email"]]

				# set meeting attendance id to equal last indix plus one
				# meeting_id is set to variable set earlier
				# attendee id is found by matching id to email
				# duration found by iterating through attendance drvs df and adding it to meeting_attendance
				meeting_attendance.loc[len(meeting_attendance.index)] = [(len(meeting_attendance.index) + 1), meeting_ids, attendee_id.iloc[0], row["Total Duration (Minutes)"]]

		#get meeting_id_df 
		meeting_id_df = meeting_id(database_df_path,attendance_drvs_df_path,cqm_focus_Yes_or_No)

		#write to excel file the new updates excel sheets
		with pd.ExcelWriter(r"C:\Users\bnguyen\Documents\LPCA\DRVS_user_attendance\drvs_att_df.xlsx") as writer:
			attendees_df.to_excel(writer, sheet_name = 'Attendees',index = False)
			meeting_attendance.to_excel(writer, sheet_name = 'Meeting_attendance',index = False)
			meeting_id_df.to_excel(writer,sheet_name='Meetings',index = False)









# create function for Meeting_attendance
# run drvs_database function on new csv's
# create df with id's of all attendees and duration for this meeting
# read in meeting_attendance df
# assign new meeting id by creating variable that is 1 += last meeting id
# attendance_id should be 1+= last attendance id
# add attendee_ID and duration either by iterating through rows or using .apply lambda


