cat part* > merged-file
cat merged-file | awk '{ print $2 "," $1}' > stopwords.csv
rm merged-file
