############################################
# INITIALIZE LAB PARAMETERS AND VARIABLES  #
############################################
CLIENT_IP="24.7.233.95" # Change this line
LAB_ENV_NAME="lab-emr-cluster"
LAB_STACK_NAME="${LAB_ENV_NAME}-stack"
LAB_KEY_NAME="${LAB_ENV_NAME}-keypair"
LAB_KEY_FILE="${LAB_KEY_NAME}.pem"
CLOUD9_PRIVATE_IP=`hostname -i`

############################################
# CREATE LAB INFRASTRUCTURE USING AWS CLI  #
############################################
aws configure set region us-east-1
export AWS_SHARED_CREDENTIALS_FILE=/home/ec2-user/.aws/credentials

CIDR_SUFFIX=
if [ "${CLIENT_IP}" = "0.0.0.0" ]; then
    CIDR_SUFFIX="/0"
else
    CIDR_SUFFIX="/32"
fi

aws ec2 create-key-pair \
    --key-name "${LAB_KEY_NAME}" \
    --query 'KeyMaterial' \
    --output text > "${LAB_KEY_FILE}"

chmod 400 "${LAB_KEY_FILE}" #change permissions

aws cloudformation deploy \
  --template-file ./EMR_Files/template.json \
  --stack-name "lab-emr-cluster-stack1" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    Name="${LAB_ENV_NAME}" \
    InstanceType=m4.large \
    ClientIP="${CLIENT_IP}${CIDR_SUFFIX}" \
    Cloud9IP="${CLOUD9_PRIVATE_IP}/32" \
    InstanceCount=4 \
    KeyPairName="${LAB_KEY_NAME}" \
    ReleaseLabel="emr-5.32.0" \
    EbsRootVolumeSize=32

############################################
# COPY FILES, CONNECT TO EMR MASTER NODE   #
############################################
LAB_CLUSTER_ID=`aws emr list-clusters --query "Clusters[?Name=='${LAB_ENV_NAME}'].Id | [1]" --output text`
aws emr wait cluster-running --cluster-id ${LAB_CLUSTER_ID}
LAB_EMR_MASTER_PUBLIC_HOST=`aws emr describe-cluster --cluster-id ${LAB_CLUSTER_ID} --query Cluster.MasterPublicDnsName --output text`

# Copy lab files to Hadoop master node
scp -i "${LAB_KEY_FILE}" EMR_Files/Sqoop/sqoop-site.xml "hadoop@${LAB_EMR_MASTER_PUBLIC_HOST}:/home/hadoop"
scp -i "${LAB_KEY_FILE}" EMR_Files/Yellow_Taxi.sql "hadoop@${LAB_EMR_MASTER_PUBLIC_HOST}:/home/hadoop" # To Load the data into mysql database
scp -i "${LAB_KEY_FILE}" EMR_Files/sqoop-import-options.txt "hadoop@${LAB_EMR_MASTER_PUBLIC_HOST}:/home/hadoop" 
scp -i "${LAB_KEY_FILE}" EMR_Files/Extracting_address.py "hadoop@${LAB_EMR_MASTER_PUBLIC_HOST}:/home/hadoop/"
scp -i "${LAB_KEY_FILE}" EMR_Files/create_table.hql "hadoop@${LAB_EMR_MASTER_PUBLIC_HOST}:/home/hadoop/"
ssh -i "${LAB_KEY_FILE}" "hadoop@${LAB_EMR_MASTER_PUBLIC_HOST}"


############################################
# LAB COMMANDS ON EMR                      #
############################################

wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-06.csv
mv yellow_tripdata_2009-06.csv yellow_taxi_data.csv

# Retrieve default username, password, and host name for mysql
MYSQL_PASSWORD=`sudo mysql --print-defaults | grep port=3306 | sed -r 's/(.+)--password=([^ ]+) --(.+)/\2/'`
MYSQL_USER=`sudo mysql --print-defaults | grep port=3306 | sed -r 's/(.+)--user=([^ ]+) --(.+)/\2/'`
MASTER_NODE_PRIVATE_HOST=`hostname -i`


# Create a keystore file to store database credentials
hadoop credential create mysql.password \
    -value changeme1 \
    -provider \
    jceks://hdfs/user/root/keystores/database.passwords.jecks 

# List keystores, make sure keystore exists  
hdfs dfs -ls /user/root/keystores 

# Copy the sqoop-site.xml to configure sqoop appropriately
sudo cp -f /home/hadoop/sqoop-site.xml \
	/usr/lib/sqoop/conf/sqoop-site.xml 

# oozie command
sh project.sh $MYSQL_PASSWORD $MYSQL_USER $MASTER_NODE_PRIVATE_HOST

############################################
# DELETE LAB RESOURCES                     #
############################################

aws cloudformation delete-stack --stack-name "lab-emr-cluster-stack"
aws cloudformation wait stack-delete-complete --stack-name "lab-emr-cluster-stack"
