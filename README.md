# NYC-Motor-Vehcile-Collision-Data-Analysis

##Data Set:
https://nycopendata.socrata.com/view/m666-sf2m

Total Rows - 769054
Source Domain - data.cityofnewyork.us
Date Created - 4/28/2014
Last Modified - 4/12/2016
Category - Public Safety
Attribution - NYPD
Owner - NYC Open Data


##Files:

* All the spark SQL queries can be found in loader.py. The output of the queries is stored in CSV files, one for each query.
* The code for GMM clustering can be found in gmm.py. The output of this algorithm is a plot showing the clusters generated. 
* The Javascript code can be found here: c3-0.4.11-/htdocs/samples/chart.html

##How to run:

loader.py and gmm.py need to be deployed on an AWS EMR cluster. 
AWS offers an easy to use dashboard for creating and launching a cluster and submitting a Spark job to it. Begin by clicking on EMR from the list of services offered on your AWS dashboard. Then click on Create Cluster and you will be directed to this page: 

![SetupImage](http://i.imgur.com/arhxIA9.png)


* Give the cluster a name, enable logging(this will be very useful to troubleshoot in case of exceptions) and select 'Cluster' in launch mode. 
* In software configuration, select Spark from the Applications list. 
* If you have not created an EC2 key-pair yet, create one, download it and enter its name here. Select default permissions and then go ahead and click on 'Create Cluster'.
You will now be directed to this page.
![Cluster Details](http://i.imgur.com/7EQguTV.png)

The cluster remains in the 'Starting' state for about 10 - 15 minutes. Once the cluster is ready for use, the status will change to 'Waiting'. The cluster is now ready to use and we can move on to uploading our data to S3.


##Uploading data

On your AWS console, from the Services dropdown on the dashboard, select S3 and you should see a S3 bucket here. It is the same bucket that is going to store logs for your cluster. Click on the S3 bucket, right click and select 'Create Folder'. Give this folder a name. Click on the folder name, from the Actions drop down, select upload and assuming you have the audioscrobbler dataset stored locally on your machine, click on 'Add files' and upload these to the folder you just created on the bucket.

You will have to SSH into your master node to copy your python code to this node. After having completed the AWS CLI setup, cd into the folder where you have your key-pair.pem saved. Copy the cluster id or the public DNS of the master node from cluster info and run any one of the following two commands:

* aws emr ssh --cluster-id <id> --key-pair-file <.pem file name>

OR

* ssh hadoop@<public dns of master> --key-pair-file <.pem file name>

Once you have sshed into your master node, create a new folder, say python_code and cd into it. Create a new file here by using the vi command, copy paste loader.py code here and save it. The code is now ready to be used by all the nodes in your cluster. Make sure to change the paths to all data files in your code.  Let us now see how to submit a job to this cluster.

##Submitting Job

From the cluster list, select the cluster you just created. Click on 'Add Step'. See the screen below for the options to configure -

![Configure Steps](http://i.imgur.com/cUzMbUt.png)

You are submitting this job using a script-runner provided by AWS. You are passing two arguments to it, the first one being "spark-submit" and the second is the path to python file you created on the master node.

Click on Add and refresh the page to see if the step was successfully added. The cluster state changes from Waiting to Running if the job is accepted and is running successfully.


