for entry in *.csv
do
        fileName=`echo $entry | cut -d'.' -f1`
        `tail -n +2 $fileName.csv > $fileName-header-less.csv`
done
