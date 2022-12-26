from tkinter import Tk
from tkinter.filedialog import askopenfilenames
import PyPDF2
import sqlite3 as db
import pandas as pd

table_dict = {
        "db_name":"pdfs.db",
        "table_name":"pdfs",
        "columns_name":["file_name","page_numbers","text_content","image_content"],
        }

def create_db_and_table():
    with db.connect(table_dict["db_name"]) as connection:
        cursor = connection.cursor()
        query = "CREATE TABLE IF NOT EXISTS "
        query += table_dict["table_name"]
        query += " ("
        query +="'" + "','".join([str(column) for column in table_dict["columns_name"]]) +"'"
        query += f", PRIMARY KEY ({table_dict['columns_name'][0]})"
        query += ");"
        print(query)
        cursor.execute(query)
        cursor.close()

def load_to_db(locations:str):
    def put_df_to_db(df:pd.DataFrame):
        with db.connect(table_dict["db_name"]) as connection:
            df = df.applymap(str)
            try:
                df.to_sql(table_dict["table_name"],connection, index=False, if_exists='append')
            except Exception :
                print("Something happened during the import into the sql db")

    for fn in locations:
        pdf_df = pd.DataFrame(columns=table_dict["columns_name"])
        text_content = []
        new_row = []
        pdf = PyPDF2.PdfFileReader(fn,strict=False)
        pages = pdf.getNumPages()
        for i in range(pages):
            text_content.append((i,pdf.getPage(i).extractText()))
        new_row.append(fn)
        new_row.append(pages)
        new_row.append(text_content)
        new_row.append([])
        pdf_df.loc[0]=new_row
        put_df_to_db(pdf_df)

def drop_tables():
    with db.connect(table_dict["db_name"]) as connection:
        cu = connection.cursor()
        cu.execute(f"DROP TABLE {table_dict['table_name']}")
        cu.close()


if __name__ == "__main__":
    Tk().withdraw()
    filenames = askopenfilenames(filetypes=[("All Files","*.pdf")])

    create_db_and_table()
    load_to_db(filenames)




    
    
    


