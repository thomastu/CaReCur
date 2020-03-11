BASEURL="ftp://ftp.ncdc.noaa.gov/pub/has/model/HAS011471083/"
LISTING="gfs-201801.txt"
while read fp;
    do
        # echo "$BASEURL""$fp"
        # echo 'gsutil cp - gs://carecur-gfs-data/'"$fp"
        gsutil -q stat gs://carecur-gfs-data/"$fp"
        return_value=$?
        if [ $return_value != 1 ]; then
            echo "$fp"" exists. Skipping."
        else
            curl "$BASEURL""$fp" | gsutil cp - gs://carecur-gfs-data/"$fp"
        fi

    done < $LISTING