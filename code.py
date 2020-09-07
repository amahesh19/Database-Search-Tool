from bsddb3 import db
import shlex
import re
from datetime import datetime

def setup():
# This function is used to open the database and,  define and return database and cursor objects. 
	rw = db.DB()
	rw.open('rw.idx', None, db.DB_HASH, db.DB_CREATE)
	rw_cur = rw.cursor()

	sc = db.DB()
	sc.open('sc.idx', None, db.DB_BTREE, db.DB_CREATE)
	sc_cur = sc.cursor()

	pt = db.DB()
	pt.open('pt.idx', None, db.DB_BTREE, db.DB_CREATE)
	pt_cur = pt.cursor()

	rt = db.DB()
	rt.open('rt.idx', None, db.DB_BTREE, db.DB_CREATE)
	rt_cur = rt.cursor()

	db_list = [rw, sc, pt, rt]
	cur_list = [rw_cur, sc_cur, pt_cur, rt_cur]
	return (db_list, cur_list)

def get_query():
# This functions takes the input from the user and returns the entire query as a string
	query = input("Please input a query (output state: brief/full, optional):")
	return query

def process_query(query, symbols, flag):
# This function removes spaces in the query and separates every word and symbol as different elements of the list
# Function also returns a bool value for the mode specified in the query
	q_list=shlex.split(query)
	for i in q_list:
		for j in symbols:
			if((len(i)>1) and (j in i)):
				if(i.startswith(j)):
					x=q_list.index(i)
					i=i.strip(j)
					q_list[x]=i
					q_list.insert(x,j)
				elif(i.endswith(j)):
					x=q_list.index(i)
					i=i.strip(j)
					q_list[x]=i
					q_list.insert(x+1,j)
				else:
					x=q_list.index(i)
					y=i.split(j)
					q_list[x]=y[0]
					q_list.insert(x+1,j)
					q_list.insert(x+2,y[1])

	#Check for output mode
	if('output' in q_list):
		ind = q_list.index('output')
		flag_val = q_list[ind+2]
		if(flag_val=='full'):
			flag=1
		elif(flag_val=='brief'):
			flag=0

		q_list.pop(ind+2)
		q_list.pop(ind)
		q_list.pop(ind)

	return (q_list,flag)

def if_date(query):
#This function checks if the input date in the query is in the correct format. 
	match = re.search('\d{4}/\d{2}/\d{2}', query)
	if( match!=None):
		return True
	else:
		return False


def extract_term(query_list, term_list, char_list):
#This functions extracts the words from the query which we need to search in pterms and rterms keys.
	ctr=0
	while(ctr<len(query_list)):
		i = query_list[ctr]
		if(i in term_list):
			query_list.pop(ctr)
			query_list.pop(ctr)
			query_list.pop(ctr)
		else:
			ctr=ctr+1

	if(len(query_list)):
		return query_list
	else:
		return 0


def handle_terms(file, term_list, cur_list):
	rid_list = []
	if(file=='pterm' or file=='rterm'):
		if('%' not in term_list[0]):
			cur_list[0].set(bytes(term_list[0], 'utf-8'))
			iter_pt = cur_list[0].current()
			while(iter_pt[0]==bytes(term_list[0], 'utf-8')):
				rid_list.append(iter_pt[1])
				iter_pt = cur_list[0].next()
		else:
			term_list[0]=term_list[0].strip('%')
			iter_pt = cur_list[0].first()
			while(iter_pt):
				if(iter_pt[0].startswith(bytes(term_list[0], 'utf-8'))):
					rid_list.append(iter_pt[1])
				iter_pt = cur_list[0].next()
	elif(file=='score'):
		if(term_list[1]=='>'):
			cur_list[0].set(bytes(term_list[0], 'utf-8'))
			iter_pt = cur_list[0].current()
			while(iter_pt):
				if(iter_pt[0]>bytes(term_list[0], 'utf-8')):
					rid_list.append(iter_pt[1])
				iter_pt = cur_list[0].next()
		if(term_list[1]=='<'):
			iter_pt = cur_list[0].first()
			while(iter_pt):
				if(iter_pt[0]<bytes(term_list[0], 'utf-8')):
					rid_list.append(iter_pt[1])
				iter_pt = cur_list[0].next()
	elif(file=='term'):
		rid_list = []
		no_of_terms = len(term_list)
		full_list = []

		for i in range(no_of_terms):
			rid_set = set()
			if "%" in term_list[i]:
				if(term_list[i].endswith("%")):
					term_list[i] = term_list[i].strip("%")
					iter_pt = cur_list[0].first()
					while(iter_pt):
						if(iter_pt[0].startswith(bytes(term_list[i],'utf-8'))):
							#print(iter_pt[1])
							rid_set.add(iter_pt[1])
						iter_pt = cur_list[0].next()
					iter_rt = cur_list[1].first()
					while(iter_rt):
						if(iter_rt[0].startswith(bytes(term_list[i],'utf-8'))):
							rid_set.add(iter_rt[1])
						iter_rt = cur_list[1].next()
				full_list.append(rid_set)

			else:
				iter_pt = cur_list[0].first()
				while(iter_pt):
					if(iter_pt[0]==bytes(term_list[i],'utf-8')):
						rid_set.add(iter_pt[1])
					iter_pt = cur_list[0].next()
				iter_rt = cur_list[1].first()
				while(iter_rt):
					if(iter_rt[0]==bytes(term_list[i],'utf-8')):
						rid_set.add(iter_rt[1])
					iter_rt = cur_list[1].next()
			full_list.append(rid_set)

		rid_list = set(full_list[0].copy())
		for i in range(1,len(full_list)):
			rid_list = full_list[i].intersection(rid_list)

	return rid_list


def extract_price(crec,price,operator,db1): 
#This function takes the candidate records as crec and filters out the records according to the price and operator from the query

    remove_records=[] #List of  records that need to be removed  from the candidate records
    for rec in crec:
        rec=str(rec).encode() #Encode a string  into bytes so as to access the database
        value=db1.get(rec)
        rec=int(rec.decode())
        value=value.decode()
        value=re.split('",|,"',value)
        pr=value[2].split(',')[0] #pr contains the price in string format
        if(pr=='unknown'):
            remove_records.append(rec)
        else:
            pr=float(pr) #Converting string to float
            if(operator=='>'):
                if(pr<=price): #If pr in record is less than price of the query, append it to the remove_records
                    if(rec not in remove_records):
                        remove_records.append(rec)
            elif(operator=='<'):
                if(pr>=price):
                    if(rec not in remove_records):
                        remove_records.append(rec)

    for i in remove_records: #Remove all the invalid records from  the candidate records and return the new list of candidate records
        crec.remove(i) 
    return crec

def extract_date(crec,date,operator,db1): 
#This function takes the candidate records as crec and filter out the records according to the date and operator from the query

    try: #input validation
        input_datetime=datetime.strptime(date,'%Y/%m/%d')
    except:
        print('Invalid format of date entered!')

    input_timestamp=datetime.timestamp(input_datetime) #coverting into timestamp format so as to compare later

    remove_records=[]
    for rec in crec:
        rec=str(rec).encode()
        value=db1.get(rec)
        rec=int(rec.decode())
        value=value.decode()
        match=re.search(',\d{10},|,\d{9},',value) #Filter out the  timestamp string from records

        if(match==None): #If the records do not consist date then the record is invalid and append it to the remove_records 
            remove_records.append(rec)
            continue
        else:
            timestamp=match.group()
            timestamp=int(timestamp[1:len(timestamp)-1])
            date_time=datetime.fromtimestamp(timestamp)
            timestamp=datetime.timestamp(date_time) #Changing into timestamp format from datetime format
            if(operator=='<'):	#Filter records out according to the conditions given below
                if(timestamp>=input_timestamp):
                    if(rec not in remove_records):
                        remove_records.append(rec)

            elif(operator=='>'):
                if(timestamp<=input_timestamp):
                    if(rec not in remove_records):
                        remove_records.append(rec)

         
    if(len(remove_records)!=0):
        for rec in remove_records:
            crec.remove(rec)

    return crec


def output(crec,db1,flag):
#This function displays the candidate rercords according to the flag value. Flag=0 is brief output. Flag=1 is full output. Default is brief output

    if(flag==0):
        for rec in crec:
            rec=str(rec).encode()
            value=db1.get(rec)
            rec=int(rec.decode())
            value=value.decode()
            listValue=re.split('",|,"',value)
            proID=listValue[1] #1 has the product title
            score=listValue[4].split(',')[1]
            print(rec,', ',proID,', ',score)
    elif(flag==1):
        cur=db1.cursor()
        for rec in crec:
            rec=str(rec).encode()
            value=db1.get(rec)
            rec=int(rec.decode())
            value=value.decode()
            print(rec,',',value)
 


def main():

	db_list, cur_list = setup()
	rw = db_list[0]
	sc = db_list[1]
	pt = db_list[2]
	rt = db_list[3]

	rw_cur = cur_list[0]
	sc_cur = cur_list[1]
	pt_cur = cur_list[2]
	rt_cur = cur_list[3]
	
	
	result_full = False
	query = ""
	isTrue=1
	flag = 0
	while(isTrue):
		try:
			ans=input('Do you want to enter a query? (Y/N): ')
			if(ans.upper()=='N'):
				isTrue=0
				print('Have a nice day!')
				break
			elif(ans.upper()=='Y'):
				pass
			else:
				print('Invalid input. Please choose from Y/N.')
				continue
			query = get_query().lower()

			term_list = ["pterm", "rterm", "score", "price", "date"]
			char_list = ['>', '<', ':','=']

			#Convert the query string into a list of different words
			#Get the output mode bool value
			query_list, flag = process_query(query, char_list, flag)

			KEY_LIST = []

			if ("pterm" in query_list):
			#If pterm in the input query gets a list of product Ids having key=given keyword in 'pt.idx' 
				p_obj = [pt_cur]
				PTERM_LIST = handle_terms("pterm", [query_list[query_list.index("pterm")+2]], p_obj)
				if(len(PTERM_LIST)):
					KEY_LIST.append(PTERM_LIST)
				index = query_list.index("pterm")
				query_list.pop(index)
				query_list.pop(index)
				query_list.pop(index)

			if ("rterm" in query_list):
			#If rterm in the input query gets a list of product Ids having key=given keyword in 'rt.idx' 
				r_obj = [rt_cur]
				RTERM_LIST = handle_terms("rterm", [query_list[query_list.index("rterm")+2]], r_obj)
				if(len(RTERM_LIST)):
					KEY_LIST.append(RTERM_LIST)
				index = query_list.index("rterm")
				query_list.pop(index)
				query_list.pop(index)
				query_list.pop(index)


			while ("score" in query_list):
			#If score in the input query gets a list of product Ids having key=given keyword in 'sc.idx' 
				s_obj = [sc_cur]
				score = str(float(query_list[query_list.index("score")+2]))
				SCORE_LIST = handle_terms("score", [score, query_list[query_list.index("score")+1]], s_obj)
				if(len(SCORE_LIST)):
					KEY_LIST.append(SCORE_LIST)
				index = query_list.index("score")
				query_list.pop(index)
				query_list.pop(index)
				query_list.pop(index)

			#Extract a list of words which we need to search in 'pt.idx' and 'rt.idx'
			query_copy = query_list.copy()
			term = extract_term(query_copy, term_list, char_list)

			if(term):
			# Get a list of product Ids having key=term in atleast one of 'pt.idx' and 'rt.idx' 
				t_obj = [pt_cur, rt_cur]
				TERMS_LIST = handle_terms("term", term, t_obj)
				if(len(TERMS_LIST)):
					KEY_LIST.append(TERMS_LIST)

			#Get intersection of all product Id's obtained till now
			RID_LIST = KEY_LIST[0].copy()
			for i in range(1,len(KEY_LIST)):
				RID_LIST = KEY_LIST[i].intersection(RID_LIST)

			#Decode the product Id's
			REC_LIST=[]
			for rec in RID_LIST:
				rec=int(rec.decode())
				REC_LIST.append(rec)

			while("price" in query_list): #Sending the required arguments to extract_price() function from the query
				index=query_list.index('price')
				operator=query_list[index+1]
				price=int(query_list[index+2])
				REC_LIST=extract_price(REC_LIST,price,operator,rw)
				query_list.pop(index)
				query_list.pop(index)
				query_list.pop(index)


			while("date" in query_list): #Sending the required arguments to extract_date() functioon from the query
				index=query_list.index('date')
				operator=query_list[index+1]
				date=query_list[index+2]
				REC_LIST=extract_date(REC_LIST,date,operator,rw)
				query_list.pop(index)
				query_list.pop(index)
				query_list.pop(index)

			print(REC_LIST)
			output(REC_LIST,rw,flag)
		except:
			print('Record Not Found! Try Another Query.')
			continue

if __name__ == '__main__':
	main()












