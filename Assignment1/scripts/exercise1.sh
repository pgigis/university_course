cat $1 | awk '{ print $2 "," $1}' > stopwords.csv
