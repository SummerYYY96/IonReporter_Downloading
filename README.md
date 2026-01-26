# IonReporter_Downlaoding  
This script downloads either VCFs or BAM file directly from ThermoFisher Ion Reporter using its API call.  
To run this script you will need a separate `.conf` file (not included here) with the following fields:  
```
[DEFAULT]
TOKEN=xxxxxxx(this is the ThermoFisher IR's API token)
HOST=xx.xxx.xx(IR's IP address)
UID=ionuser
BAM_DOWNLOADS_DIR=/directory/where/you/want/to/save/the/bam
VAR_DIR=/directory/where/you/want/to/save/the/vcfs
```

To run the script do  
```
#VCF
python downloading_files.py --config config.conf --sample SAMPLE123 --variants
#BAM
python downloading_files.py --config config.conf --sample SAMPLE123 --bams
```
