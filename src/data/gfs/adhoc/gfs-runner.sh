YEAR=2019
for MONTH in {2..5}
    do
        python src/data/gfs/parse_ca.py carecur --year=$YEAR --month=$MONTH
        # echo "python src/data/gfs/parse_ca.py carecur --year=$YEAR --month=$MONTH"
    done