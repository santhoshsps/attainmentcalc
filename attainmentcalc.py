import pandas as pd
from google.colab import auth
import re

auth.authenticate_user()
# import the gspread library
import gspread
from gspread_dataframe import set_with_dataframe
from google.auth import default
creds, _ = default()
gc = gspread.authorize(creds)

def open_spreadsheet(ss_url):
  ss_id = ss_url.split('/d/')[1].split('/edit')[0]
  spreadsheet = gc.open_by_key(ss_id)
  return spreadsheet

def read_sheet(spreadsheet, sheet_name):
  worksheet = spreadsheet.worksheet(sheet_name)
  data = worksheet.get_all_values()
  df = pd.DataFrame(data, columns=data[2])
  return df


def find_row_index(df, pattern):
  row = df[df.apply(lambda row: row.astype(str).str.contains(pattern).any(), axis=1)].index.tolist()
  return row[0]

def read_co_pso_mapping(spreadsheet, sheet_name):
  df = read_sheet(spreadsheet,sheet_name)
  row_start = find_row_index(df,'CO-PSO Mapping')+1
  po_list =  list(df.iloc[row_start,1:])
  co_list =  list(df.iloc[row_start+1:,0])
  df = df.iloc[row_start+1:,1:]
  df.columns = po_list
  df.index = co_list
  df[df==''] = 0
  df = df.astype(float)
  return df, po_list, co_list

def read_assessment(spreadsheet, sheet_name):
  df = read_sheet(spreadsheet,sheet_name)
  assessment_type = df.iloc[find_row_index(df,'Type of Assessment'),1]
  id = df.iloc[find_row_index(df,'Assessment ID'),1]
  weight = (df.iloc[find_row_index(df,'Weight'),1])

  col_start = 2
  #find the course outcomes
  row_co = find_row_index(df,'CO')
  co_list =  df.iloc[row_co,col_start:]

  row_marks = find_row_index(df,'Marks')

  row_start = find_row_index(df,'Candidate Code')
  question_list = df.iloc[row_start, col_start:]

  question_list_for_co = {}
  marks_for_co = {}
  co_marklist = pd.DataFrame()

  row_start = row_start+1 # Data (Marks) starts here

  for co in co_list:
    selected_indices = co_list==co
    question_list_for_co[co] = list(question_list[selected_indices])
    marks_for_co[co] = df.iloc[row_marks,col_start:][selected_indices].apply(pd.to_numeric).sum()

    for i in range(row_start,df.shape[0]):
      co_marklist.loc[i-row_start,'Code'] = df.iloc[i,0]
      co_marklist.loc[i-row_start,'Name'] = df.iloc[i,1]
      marks_obtained = df.iloc[i,col_start:][selected_indices].apply(pd.to_numeric).sum()
      co_marklist.loc[i-row_start,co] = marks_obtained/marks_for_co[co]

  return co_marklist, assessment_type, id, weight


#course
class course:
  def __init__(self, spreadsheet_url):
    self.spreadsheet_url = spreadsheet_url
    self.assessments = []
    self.spreadsheet = open_spreadsheet(spreadsheet_url)
    df_general = read_sheet(self.spreadsheet,'General')
    row = find_row_index(df_general,'Attainment Level')
    levels = list(df_general.iloc[row+1,:].apply(pd.to_numeric))
    values = list(df_general.iloc[row+2,:].apply(pd.to_numeric))
    attainment_levels = {}
    for i in range(len(levels)-1):
      attainment_levels[levels[i]] = values[i]
    row = find_row_index(df_general,'CO-PO Map')
    self.CO_PO_sheet = df_general.iloc[row,1]
    row = find_row_index(df_general,'CO-PSO Map')
    self.CO_PSO_sheet = df_general.iloc[row,1]
    row = find_row_index(df_general,'Course ID')
    self.course_id = df_general.iloc[row,1]
    row = find_row_index(df_general, 'Course Name')
    self.course_name = df_general.iloc[row,1]
    row = find_row_index(df_general, 'Assessment Sheets')
    assessment_sheet_list = list(df_general.iloc[row,1:])
    row = find_row_index(df_general, 'Assessment Types')
    
    values = list(df_general.iloc[row+1,1:].apply(pd.to_numeric))
    keys = list(df_general.iloc[row,1:])
    self.assessment_weights = {}
    for i in range(len(keys)-1):
      self.assessment_weights[keys[i]] = float(values[i])
    
    row = find_row_index(df_general, 'Threshold')
    self.threshold = float(df_general.iloc[row,1])

    self.total_weight = 0
    self.num_assessments_type ={}
    for assessment_type in self.assessment_weights.keys():
      self.num_assessments_type[assessment_type] = 0
      self.total_weight += self.assessment_weights[assessment_type]

    for sheet_name in assessment_sheet_list:
      if not sheet_name:
        continue
      co_marklist, assessment_type, assessment_id, weight = read_assessment(self.spreadsheet,sheet_name)
      co_marklist.index = co_marklist['Code']
      co_marklist.drop('Code',axis=1,inplace=True)
      co_marklist = co_marklist[co_marklist.index != 'Code']
      self.add_assessments(assessment_type, assessment_id, weight, co_marklist)
      self.num_assessments_type[assessment_type] += 1

    self.result = self.assessments[0]['marklist'].copy()
    selected_colums = self.assessments[0]['marklist'].columns.difference(['Name'])

    self.assessments_consolidated ={}
    for assessment_type in self.assessment_weights.keys():
      self.assessments_consolidated[assessment_type] = self.assessments[0]['marklist'].copy()
      self.assessments_consolidated[assessment_type][selected_colums] = 0
      
    for assessment in self.assessments:
      assessment_type = assessment['type']
      self.assessments_consolidated[assessment_type][selected_colums] += assessment['marklist'][selected_colums]

    for assessment_type in self.assessment_weights.keys():
      self.assessments_consolidated[assessment_type][selected_colums] /= self.num_assessments_type[assessment_type]
    self.assessments_consolidated_without_weigth = self.assessments_consolidated.copy()

    for assessment_type in self.assessment_weights.keys():
      self.assessments_consolidated[assessment_type][selected_colums] *= self.assessment_weights[assessment_type]


    assessment_name = list(self.assessments_consolidated.keys())[0]
    self.result = self.assessments_consolidated[assessment_name].copy()
    self.result[selected_colums] = 0
    for assessment_type in self.assessment_weights.keys():
      self.result[selected_colums] += self.assessments_consolidated[assessment_type][selected_colums]
    self.result[selected_colums] /= self.total_weight


    self.attainment = pd.DataFrame(index = ["Attainment %","Attainment Level"],
                                              columns = self.result.columns[1:])
    self.attainment[:] = 0
    total_students = self.result.shape[0]

    for column in selected_colums:
      students_above_threshold = self.result[column][self.result[column] >= self.threshold].count()
      self.attainment.loc["Attainment %",column] = \
        round(students_above_threshold*100/total_students,2)
      for level in attainment_levels.keys():
        if self.attainment.loc["Attainment %",column] >= attainment_levels[level]:
          self.attainment.loc["Attainment Level",column] = level
          break
    self.pso_attainment_data = self.compute_co_pso(self.CO_PSO_sheet)
    self.po_attainment_data = self.compute_co_pso(self.CO_PO_sheet)


  def add_assessments(self, assessment_type, assessment_id, weight, marklist):
      self.assessments.append({'type':assessment_type,'id':assessment_id,\
                               'weight':float(weight),'marklist':marklist})
  def compute_co_pso(self,sheet):
    df, pso_list, co_list = read_co_pso_mapping(self.spreadsheet,sheet)
    attainment = df.copy()
    attainment[:] = 0
    for po in pso_list:
      for co in co_list:
        if co in self.attainment.columns:
         attainment.loc[co,po] = round(df.loc[co,po]*self.attainment.loc['Attainment %',co]/100,2)
    total = attainment.sum()
    attainment_max = df.sum()
    percent = attainment_max.copy()
    selected = attainment_max != 0
    percent[selected] = total[selected]*100/attainment_max[selected]
    attainment.loc['Total',:] = total
    attainment.loc['%'] = percent
    attainment.loc['%'] = attainment.loc['%'].map(lambda x: round(x,2))
    return attainment
  '''
  def compute_co_pso(self,sheet):
    df, pso_list, co_list = read_co_pso_mapping(self.spreadsheet,sheet)
    course_co_list = self.attainment.columns
    pso_attainment_data = pd.DataFrame(columns =pso_list, index = course_co_list)
    pso_attainment_data[:] = 0
    pso_total = {}
    for pso in pso_list:
      pso_total[pso] = 0
      for co in course_co_list:
        pso_total[pso] += df.loc[co,pso]
        pso_attainment_data.loc[co,pso] = round(df.loc[co,pso]*self.attainment.loc['Attainment %',co]/100,2)
    pso_sum = pso_attainment_data.sum().apply(pd.to_numeric)
    pso_percent = {}
    for pso in pso_list:
      if pso_total[pso] != 0:
        pso_percent[pso] = round( pso_sum[pso]*100/pso_total[pso],2)
      else:
        pso_percent[pso] = 0
    pso_attainment_data.loc['Total',:] = pso_sum
    pso_attainment_data.loc['%'] = pso_percent
    return pso_attainment_data
  '''

  def write_attainment(self,sheet_name,verbose=True):
    if verbose:
      print(f"\nWriting Attainment for course '{self.course_id}:{self.course_name}'\
       \nin sheet '{sheet_name}' ")
    from gspread_dataframe import set_with_dataframe
    self.rows =[]
    self.row = 2
    def append_df_with_title(dfs,df,title):
      title_df = pd.DataFrame({'Name':[title]}, index = [''])
      dfs.extend([title_df, df, pd.DataFrame()])
      self.row += df.shape[0]+1
      self.rows.append(self.row)
      return dfs
    try:
      newsheet = self.spreadsheet.worksheet(sheet_name)
      self.spreadsheet.del_worksheet(newsheet)
    except gspread.exceptions.WorksheetNotFound:
      pass
    newsheet = self.spreadsheet.add_worksheet(title=sheet_name,rows=100,cols=20)
    dfs = []
    self.rows.append(self.row)
    dfs = append_df_with_title(dfs,self.result, "Results")
    dfs = append_df_with_title(dfs,self.attainment, "Attainment")

    for assessment_type in self.assessment_weights.keys():
      df = self.assessments_consolidated_without_weigth[assessment_type]
      title = f"Assessment Type: {assessment_type}"
      dfs = append_df_with_title(dfs,df,title)
      if verbose:
        print(f"\nWritten '{assessment_type} Assessment' ")

    if verbose:
      print(f'\nWriting Individual Assessments')
    for assessment in self.assessments:
      df = assessment['marklist']
      title = f"Assessment Type: {assessment['type']}, Assessment ID: {assessment['id']}"
      dfs = append_df_with_title(dfs,df,title)
      if verbose:
        print(f"\nWritten '{assessment['type']}' with Assessment ID: {assessment['id']}")

    final_df = pd.concat(dfs)
    final_df.index.name = 'Candidate Code'
    # Write the DataFrame to the worksheet
    set_with_dataframe(worksheet=newsheet, dataframe=final_df,
                       include_index=True, include_column_header=True)
    # format the header row
    col = chr(64+final_df.shape[1]+1)
    range = f'A1:{col}1'
    newsheet.format(range,\
                    {"backgroundColor":{"red": 0.8, "green": 0.9, "blue": 0.8},
                     "textFormat":{"bold": True, "fontSize": 12}})

    #format header rows
    for row in self.rows:
      range = f'A{row}:{col}{row}'
      newsheet.format(range,\
                      {"backgroundColor":{"red": 0.8, "green": 0.8, "blue": 0.8},
                       "textFormat":{"bold": True, "italic": True, "fontSize": 12}})
    newsheet.format(f'C:{col}',{"horizontalAlignment":"CENTER"})

    if verbose:
      url = self.spreadsheet_url.split('/edit')[0] + f'#gid={newsheet.id}'
      print(f"\nWritten CO Attainment to worksheet:'{sheet_name}'\
      \nin Spreadsheet '{self.spreadsheet.title}' [{url}] \n")

    return newsheet

  def write_co_po_pso_attainment(self,sheet_name,verbose=True):
    if verbose:
      print(f"\n\nWriting PO and PSO Attainments to worksheet '{sheet_name}' ")
     
    from gspread_dataframe import set_with_dataframe
    self.rows =[]
    self.row = 2
    def append_df_with_title(dfs,df,title):
      title_df = pd.DataFrame({'':[title]}, index = ['CO'])
      dfs.extend([title_df, df, pd.DataFrame()])
      self.row += df.shape[0]+1
      self.rows.append(self.row)
      return dfs
    try:
      newsheet = self.spreadsheet.worksheet(sheet_name)
      self.spreadsheet.del_worksheet(newsheet)
    except gspread.exceptions.WorksheetNotFound:
      pass
    newsheet = self.spreadsheet.add_worksheet(title=sheet_name,rows=100,cols=20)
    dfs = []
    self.rows.append(self.row)
    dfs = append_df_with_title(dfs,self.po_attainment_data, "PO Attainmnet")
    dfs = append_df_with_title(dfs,self.pso_attainment_data, "PSO Attainment")

    final_df = pd.concat(dfs, axis = 1)
    set_with_dataframe(worksheet=newsheet, dataframe=final_df,
                       include_index=True, include_column_header=True)
    
    # format the header row
    col = chr(64+final_df.shape[1]+1)
    range = f'A1:{col}1'
    newsheet.format(range,\
                    {"backgroundColor":{"red": 0.8, "green": 0.9, "blue": 0.8},
                     "textFormat":{"bold": True, "fontSize": 12}})
    range = f'A{self.rows[0]}:{col}{self.rows[0]}'
    newsheet.format(range,\
                    {"backgroundColor":{"red": 0.8, "green": 0.8, "blue": 0.8},
                     "textFormat":{"bold": True, "fontSize": 12}})
    newsheet.format(f'C:{col}',{"horizontalAlignment":"CENTER"}) 
    range = f'A{self.rows[1]-2}:{col}{self.rows[1]-1}'
    newsheet.format(range,\
                    {"backgroundColor":{"red": 0.95, "green": 0.95, "blue": 0.8},
                     "textFormat":{"bold": True, "fontSize": 12}})
    '''
    #format header rows
    for row in self.rows:
      range = f'A{row}:{col}{row}'
      newsheet.format(range,\
                      {"backgroundColor":{"red": 0.8, "green": 0.8, "blue": 0.8},
                       "textFormat":{"bold": True, "italic": True, "fontSize": 12}})
    newsheet.format(f'C:{col}',{"horizontalAlignment":"CENTER"})
    '''
    if verbose:
      url = self.spreadsheet_url.split('/edit')[0]+f'#gid={newsheet.id}'
      print(f"\nWritten PO and PSO Attainments to worksheet:'{sheet_name}' \
       \nin Spreadsheet '{self.spreadsheet.title}' [{url}] \n")
    return newsheet
