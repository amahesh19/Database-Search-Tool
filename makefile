all: Run 

SortUnique: 
	sort -u -o scores.txt scores.txt
	sort -u -o pterms.txt pterms.txt
	sort -u -o rterms.txt rterms.txt
	
Phase1: SortUnique
	perl break.pl < reviews.txt > reviews1.txt
	perl break.pl < scores.txt > scores1.txt
	perl break.pl < pterms.txt > pterms1.txt
	perl break.pl < rterms.txt > rterms1.txt
	rm -f rw.idx
	rm -f pt.idx
	rm -f rt.idx
	rm -f sc.idx
	db_load -T -t hash -f reviews1.txt -c duplicates=1 rw.idx
	db_load -T -t btree -f pterms1.txt -c duplicates=1 pt.idx
	db_load -T -t btree -f rterms1.txt -c duplicates=1 rt.idx
	db_load -T -t btree -f scores1.txt -c duplicates=1 sc.idx
	
Run:	Phase1 
	python3 code.py

