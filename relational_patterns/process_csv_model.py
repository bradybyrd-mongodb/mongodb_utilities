import sys
import os
import csv
import re
import random
from collections import defaultdict
import time
import pprint
base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
bb = Util()
    
'''
    # -------------------------------------------------------------- #
    #      csv Processor
    # -------------------------------------------------------------- #
    #   Reads model csvs and produces template for either
    #   relational or mongodb documents
    #   NOTE: only support 5-levels of depth

File Format Example:
Resource.Property,Property Type,Generator
Product.product_id,String,"IDGEN.get(""PR-"")"
Product.name,String,"fake.bs()",Smith
Product.description,text,"fake.paragraph()",Sam
Product.category,String,"fake.random_element(('Medicare', 'Medicaid', 'PPO', 'HMO','Major Medical'))"
Product.eligibility,String,"fake.bs()"
Product.isActive,String,"fake.random_element(('T', 'F',))"
Product.startDate,date,"fake.past_datetime()"
Product.endDate,date,"fake.past_datetime()"
Product.premium,double,"fake.random_int(min=200, max=1000)"
Product.coverage(10).name,String,"fake.bs()"
Product.coverage(10).description,text,"fake.paragraph()"
Product.coverage(10).coverageType,String,"fake.bs()"
Product.coverage(10).deductibleConditions().conditionName,String,"fake.bs()"
    
    
'''

def ddl_from_template(template, domain):
    bb.message_box("Generating DDL")
    database = "default"
    # Read the csv file and digest
    fields = fields_from_template(template)
    # pprint.pprint(fields)
    tables = {}
    last_table = "zzzzz"
    ddl = ""
    for row in fields:
        table, field, ftype = clean_field_data(row)
        if table not in tables:
            # bb.logit("#--------------------------------------#")
            bb.logit(f"Building table: {table}")
            last_table = table
            fkey = ""
            flds = []
            if len(table.split("_")) > 1:
                #  Add a parent_id field
                new_field = stripProp(f'{row["parent"]}_id')
                fkey = f"  {new_field} varchar(20) NOT NULL,"
                flds.append(new_field)
                #  Add a self_id field
                new_field = stripProp(f"{table}_id")
                fkey += f"  {new_field} varchar(20) NOT NULL,"
                flds.append(new_field)
            flds.append(field)
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {table} ("
                "  id SERIAL PRIMARY KEY,"
                f"{fkey}"
                f"  {field} {ftype},"
            )
            tables[table] = {
                "ddl": ddl,
                "database": database,
                "fields": flds,
                "generator": [row["generator"]],
                "parent": row["parent"],
                "sub_size" : row["sub_size"]
            }

        else:
            # bb.logit(f'Adding table data: {table}, {field}')
            if field not in tables[table]["fields"]:
                tables[table]["ddl"] = tables[table]["ddl"] + f"  {field} {ftype},"
                tables[table]["fields"].append(field)
                tables[table]["generator"].append(row["generator"])
        first = False
    clean_ddl(tables)
    bb.logit("Table DDL:")
    pprint.pprint(tables)
    return tables

def master_from_file(file_name):
    return file_name.split("/")[-1].split(".")[0]

def clean_field_data(data):
    tab = data["table"]
    if data["name"].lower() == "id":
        data["name"] = f"{tab}_id"
    if (
        len(tab.split("_")) == 2 and tab.split("_")[0] == tab.split("_")[1]
    ):  # "catch doubled eg member_member"
        tab = tab.split("_")[0]
    return tab, data["name"], data["type"]

def clean_ddl(tables_obj):
    for tab in tables_obj:
        ddl = tables_obj[tab]["ddl"]
        l = len(ddl)
        ddl = ddl[: l - 1] + ")"
        tables_obj[tab]["ddl"] = ddl
        fmts = value_codes(tables_obj[tab]["fields"])
        tables_obj[tab][
            "insert"
        ] = f'insert into {tab} ({",".join(tables_obj[tab]["fields"])}) values ({fmts});'

def pg_type(mtype):
    type_x = {
        "string": "varchar(100)",
        "boolean": "varchar(2)",
        "date": "timestamp",
        "integer": "integer",
        "text": "text",
        "double": "real",
    }
    ftype = type_x[mtype.lower().strip()]
    return ftype

def value_codes(flds, special={}):
    result = ""

    for i in range(len(flds)):
        fmt = "%s"
        if i in special:
            fmt = special[i]
        if i == 0:
            result = fmt
        else:
            result += f", {fmt}"
    return result

def fields_from_template(template):
    """
    {'name': 'EffectiveDate', 'table': 'member_address', 'type': 'date', 'generator' : "fake.date()", 'parent' : 'member'},
    """
    ddl = []
    with open(template) as csvfile:
        #propreader = csv.reader(itertools.islice(csvfile, 1, None))
        propreader = csv.reader(csvfile)
        master = ""
        sub_size = 1
        next(propreader)
        # support for parent.child.child.field, parent.children().field
        for row in propreader:
            #print(row)
            path = row[0].split(".")
            if "CONTROL" in row[0]:
                continue
            result = {
                "name": "",
                "table": "",
                "parent": "",
                "type": pg_type(row[1]),
                "generator": row[2],
                "sub_size" : sub_size
            }
            depth = len(path)
            sub_size = 1
            for k in range(depth):
                if path[k].endswith(')'):
                    res = re.findall(r'\(.*\)',path[k])[0]
                    ttype = "array"
                    if res != "()":
                        sub_size = int(res.replace("(","").replace(")",""))
                    else:
                        sub_size = random.randint(2,5)
                    path[k] = path[k].replace(res,"")
                    
            result["name"] = path[-1]
            result["sub_size"] = sub_size
            master = f"{path[0]}"
            if depth == 1:
                bb = ""  # should never see this
            elif depth == 2:
                result["table"] = master
            elif depth == 3:
                result["table"] = f"{path[0]}_{path[1]}"
                result["parent"] = master
            elif depth == 4:
                result["table"] = f"{path[0]}_{path[1]}_{path[2]}"
                result["parent"] = f"{path[0]}_{path[1]}"
            elif depth == 5:
                result["table"] = f"{path[0]}_{path[1]}_{path[2]}_{path[3]}"
                result["parent"] = f"{path[0]}_{path[1]}_{path[2]}"
            ddl.append(result)
    return ddl

def master_from_file(file_name):
    return file_name.split("/")[-1].split(".")[0]

def doc_from_template(template, domain):
    design = ddl_from_template(template, domain)
    doc = doc_from_csv(design)
    return doc

def doc_from_csv(design):
    doc = {}
    doc_name = "none"
    print(f'# --------------------------- Starting Doc ----------------------- #')
    icnt = 0
    for key in design:
        parts = key.split("_")
        sub_size = design[key]["sub_size"]
        if icnt == 0: 
            doc_name = key
        print(f'Table: {doc_name}|{key}, cnt: {sub_size}')
        if len(parts) == 1: # ROOT of doc
            doc = generate_field_values(design[key], doc_name, icnt)
        elif len(parts) == 2:
            doc[parts[1]] = generate_field_values(design[key], doc_name, icnt)
        elif len(parts) == 3:
            pprint.pprint(doc[parts[1]])
            ecnt = 0
            for item in doc[parts[1]]:
                doc[parts[1]][ecnt][parts[2]] = generate_field_values(design[key], doc_name, icnt)
                ecnt += 1
        elif len(parts) == 4:
            ecnt = 0
            for item in doc[parts[1]]:
                fcnt = 0
                for fitem in item:
                    doc[parts[1]][ecnt][parts[2]][fcnt] = generate_field_values(design[key], doc_name, icnt)
                    fcnt += 1
                ecnt += 1
    
        elif len(parts) == 5:
            ecnt = 0
            for item in doc[parts[1]]:
                fcnt = 0
                for fitem in item:
                    gcnt = 0
                    for gitem in fitem:
                        doc[parts[1]][ecnt][parts[2]][fcnt][parts[3]][gcnt] = generate_field_values(design[key], doc_name, icnt)
                        gcnt += 1
                    fcnt += 1
                ecnt += 1     
        icnt += 1
        #pprint.pprint(doc, sort_dicts=False)
    return(doc)

def generate_field_values(subdesign, doc_name, icnt):
    subs = []
    root = {}
    sub_size = subdesign["sub_size"]
    for iters in range(sub_size):
            scnt = 0
            for fld in subdesign["fields"]:
                if icnt > 0 and fld.lower().startswith(doc_name.lower()) and fld.endswith("_id"):
                    continue
                else:
                    try:
                        root[fld] = subdesign["generator"][scnt]
                    except Exception as e:
                        print(f"ERROR: field: {fld}, {scnt}")
                    scnt += 1
            subs.append(root)
    subs = subs[0] if sub_size == 1 else subs
    return subs
#----------------------------------------------------------------------#
#   CSV Loader Routines
#----------------------------------------------------------------------#
#stripProp = lambda str: re.sub(r'\s+', '', (str[0].lower() + str[1:].strip('()')))
def stripProp(str):
    ans = str
    if str[0].isupper() and str[1].islower():
        ans = str[0].lower() + str[1:]
    if str.endswith(")"):
        stg = re.findall(r'\(.*\)',ans)[0]
        ans = ans.replace(stg,"")
    ans = re.sub(r'\s+', '', ans)
    return ans

#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    ARGS = bb.process_args(sys.argv)
    #settings = bb.read_json(settings_file)
    #base_counter = settings["base_counter"]
    #IDGEN = Id_generator({"seed" : base_counter})
    id_map = defaultdict(int)
    if "wait" in ARGS:
        interval = int(ARGS["wait"])
        if interval > 10:
            bb.logit(f'Delay start, waiting: {interval} seconds')
            time.sleep(interval)
    #conn = client_connection()
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "show_ddl":
        action = "none"
        template = ARGS["template"]
        domain = "bugsy"
        ddl_from_template(action, template, domain)
    elif ARGS["action"] == "show_design":
        action = "none"
        template = ARGS["template"]
        domain = "bugsy"
        design = ddl_from_template(action, template, domain)
        doc_design = doc_from_csv(design)
        pprint.pprint(doc_design, sort_dicts=False)
    