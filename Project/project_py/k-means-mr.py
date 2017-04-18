# Initiate environment
import pandas as pd
import random
import numpy as np
from time import time
from operator import add


import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row

from pyspark.sql import *
from pyspark.sql.types import *

conf = SparkConf().setAppName("NikitaBaranov.Project").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


sqlContext = SQLContext(sc)


# Constants

folder = "uc_kep_obs"
file_to_clusterize = "temp_uc_kep_obs.txt_2000-100.csv"
file_name = str(folder)+"/"+str(file_to_clusterize)

# Number of clusters
num_of_rows = 1000
number_of_clusteroids = 200
points_to_learn = number_of_clusteroids / 5
#year_slice = 2016

# Clusteroid ID field
clusteroid_id_field = "id_org"
cluster_field = "Cluster"
cluster_distance_to_clusteroid_field = "Cluster_distance"


# Columns to compare
columns_to_count_distance=[    "is_head",
    "is_client",
    "is_ke_abonent",
    "is_ke_head",
    "stts",
    "okved",
    "region_code",
    "cptl",
    "filial_cnt", #to 0
#    "manager", # delete?
    "have_asv_max",
    "have_asv_min",
    "have_bk",
    "have_diadoc",
    "have_edi",
    "have_alko",
    "have_egais",
    "have_kz",
    "have_fms",
    "have_kn",
    "have_kf",
    "have_ke",
    "have_kep",
    "have_kep_egais",
    "have_nds",
    "have_etp",
    "have_elba",
    "have_bk_abonent",
    "have_rpn",
    "have_kemb",
    "have_free_kf",
    "have_otchetru",
    "have_kd",
    "have_adk_plus",
    "have_focus_api",
    "since_asv_max",
    "upto_asv_max",
    "sc_asv_max",
    "tp_asv_max",
    "since_asv_min",
    "upto_asv_min",
    "sc_asv_min",
    "tp_asv_min",
    "since_bk",
    "upto_bk",
    "sc_bk",
    "tp_bk",
    "since_diadoc",
    "upto_diadoc",
    "sc_diadoc",
    "tp_diadoc",
    "since_edi",
    "upto_edi",
    "sc_edi",
    "tp_edi",
    "since_alko",
    "upto_alko",
    "sc_alko",
    "tp_alko",
    "since_egais",
    "upto_egais",
    "sc_egais",
    "tp_egais",
    "since_kz",
    "upto_kz",
    "sc_kz",
    "tp_kz",
    "since_fms",
    "upto_fms",
    "sc_fms",
    "tp_fms",
    "since_kn",
    "upto_kn",
    "sc_kn",
    "tp_kn",
    "since_kf",
    "upto_kf",
    "sc_kf",
    "tp_kf",
    "since_ke",
    "upto_ke",
    "sc_ke",
    "tp_ke",
    "since_kep",
    "upto_kep",
    "sc_kep",
    "tp_kep",
    "since_kep_egais",
    "upto_kep_egais",
    "sc_kep_egais",
    "tp_kep_egais",
    "since_nds",
    "upto_nds",
    "sc_nds",
    "tp_nds",
    "since_etp",
    "upto_etp",
    "sc_etp",
    "tp_etp",
    "since_kemb",
    "upto_kemb",
    "sc_kemb",
    "tp_kemb",
    "fns",
    "pfr",
    "fss",
    "rosstat",
    "fns1151001",
    "fns1151006",
    "fns1151020",
    "fns1151054",
    "fns1151059",
    "fns1151072",
    "fns1151078",
    "fns1152004",
    "fns1152016",
    "fns1152017",
    "fns1152026",
    "fns1152028",
    "fns1153005",
    "fns1151038",
    "fns1151082",
    "fns1151085",
    "fns_not_report",
    "fns_debt",
    "kf_rightokved_ivanova",
    "kb_kopf_error",
    "kb_pfr_programm",
    "ofd_good",
    "isbeer_asv",
#    "non_food_retail",
#    "food_retail",
#    "cash_register_count",
#    "cash_register_count_in_reestr",
#    "cash_register_count_not_in_reestr",
    "isbill_kn",
    "isbill_kf",
    "isbill_kf_api",
    "isbill_ke",
    "isbill_rpn",
    "isbill_kemb",
    "isbill_ksnds",
    "isbill_kopf",
    "isbill_diadoc",
    "isbill_kep_egais",
    "isbill_kontur_egais",
    "isbill_kep",
    "isbill_adk",
    "isbill_asv",
    "isbill_buhta",
    "isbill_ofd",
    "isbill_uc",
    "isbill_edi",
    "isbill_kz",
    "isbill_fms",
    "isbill_kd",
    "isbill_evrika",
    "isbill_elba",
    "isbill_school",
#    "bill_kn",
#    "bill_kf",
#    "bill_kf_api",
#    "bill_ke",
#    "bill_rpn",
#    "bill_kemb",
#    "bill_ksnds",
#    "bill_kopf",
#    "bill_diadoc",
#    "bill_kep_egais",
#    "bill_kontur_egais",
#    "bill_kep",
#    "bill_adk",
#    "bill_asv",
#    "bill_buhta",
#    "bill_ofd",
#    "bill_uc",
#    "bill_edi",
#    "bill_kz",
#    "bill_fms",
#    "bill_kd",
#    "bill_evrika",
#    "bill_elba",
#    "bill_school",
    "active_ke_users",
    "fns_system_envd",
    "fns_system_usn",
    "fns_system_osno",
    "include_ob_adk",
    "include_ob_asv",
    "have_adk_ob",
    "have_asv_ob",
    "have_kfapi",
    "have_shet",
    "reg_bk",
    "reg_elba",
    "nds_activ",
#    "buh",
#    "dir",
    "kf_cnt_user",
    "demo_kopf_date",
    "child_ob",
    "ob",
    "cnt_ob_cells",
    "isBudgetary",
#    "okopf_99",
#    "okopf_2012",
#    "opf_summ",
    "personal", # collect into chanks
#    "cnt_founded_ul",
    "cnt_founder_ul", # collect into chanks
    "cnt_founder_fl", # collect into chanks
    "revenue",
    "revenue2014", # collect into chanks
    "revenue2015", # collect into chanks
    "profit2014", # collect into chanks
    "okved_desc",
    "otrasl",
    "segment",
    "role",
    "arbitrage_c", # collect into chanks
    "arbitrage_c_lastyear", # collect into chanks
    "arbitrage_d",# collect into chanks
    "arbitrage_cd",# collect into chanks
    "gov_customer",
    "gov_supplier",
    "etp_activity_bd",
    "etp_activity_ed",
    "etp_cnt",# collect into chanks
    "etp_accredit_bd",
    "etp_accredit_ed",
    "etp_mmvb",
    "etp_roseltorg",
    "etp_rts",
    "etp_sbast",
    "etp_zakazrf",
#    "etp_acc_cluster",
    "all_prolongation_for_etp",
    "cert_etprf_ac",
    "cert_etprf_upto",
#    "cert_etprf_fio",
    "cert_sb_ac",
    "cert_sb_upto",
    "giszkh",
#    "giszkh_manager",
    "lesegais",
    "fsrar_license",
    "alko_shipper_cnt",# collect into chanks
#    "alko_shipper_big",
    "rpn_reestr",
    "fz44_customer",
#    "fz44_customer_name",
#    "fz44_supplier",
#    "nostroy_member",
#    "nopriz_member",
    "OGV",
    "ka_egais",
    "obpit",
    "bank",
    "gp",
    "mfo",
    "ssd",
    "lising",
    "suppliers_44_223",
    "single_supplier44",
    "single_supplier223",
    "summ_contracts44",# collect into chanks
    "summ_contracts223",# collect into chanks
#    "comp_kz",
    "fns_registerdisqualified",#check
    "fns_invalidaddresses",#check
    "oos_unfair_suppliers",#check
    "cnt_listid_edi",
    "cnt_listid_adk",
    "cnt_listid_amba",
    "cnt_listid_asv",
    "cnt_listid_dd",
    "cnt_listid_infbez",
    "cnt_listid_kb",
    "cnt_listid_kba",
    "cnt_listid_kbb",
    "cnt_listid_kd",
    "cnt_listid_kz",
    "cnt_listid_kn",
    "cnt_listid_kpd",
    "cnt_listid_kpers",
    "cnt_listid_ks",
    "cnt_listid_nds",
    "cnt_listid_kf",
    "cnt_listid_kfapi",
    "cnt_listid_fms",
    "cnt_listid_ke",
    "cnt_listid_kemb",
    "cnt_listid_kep",
    "cnt_listid_rpn",
    "cnt_listid_seminar",
    "cnt_listid_fst",
    "cnt_listid_shb",
    "cnt_listid_shet",
    "cnt_listid_etp",
    "cnt_listid_all",
#    "load_lpr1",
#    "load_lpr2",
#    "load_lpr3",
#    "load_lpr5",
#    "load_lpr4_edi",
#    "load_lpr4_kep",
#    "load_lpr4_kd",
#    "load_lpr4_kn",
#    "load_lpr4_fms",
#    "load_lpr4_fst",
#    "load_lpr4_kfapi",
#    "load_lpr4_shet",
#    "load_lpr4_ofd",
#    "load_sc_edi",
#    "load_sc_adk",
#    "load_sc_amba",
#    "load_sc_asv",
#    "load_sc_dd",
#    "load_sc_infbez",
#    "load_sc_kb",
#    "load_sc_kba",
#    "load_sc_kbb",
#    "load_sc_kd",
#    "load_sc_kz",
#    "load_sc_kn",
#    "load_sc_kpd",
#    "load_sc_kpers",
#    "load_sc_ks",
#    "load_sc_nds",
#    "load_sc_kf",
#    "load_sc_kfapi",
#    "load_sc_fms",
#    "load_sc_ke",
#    "load_sc_kemb",
#    "load_sc_kep",
#    "load_sc_rpn",
#    "load_sc_seminar",
#    "load_sc_fst",
#    "load_sc_shb",
#    "load_sc_shet",
#    "load_sc_etp",
#    "hold_edi",
#    "hold_adk",
#    "hold_amba",
#    "hold_asv",
#    "hold_dd",
#    "hold_infbez",
#    "hold_kb",
#    "hold_kba",
#    "hold_kbb",
#    "hold_kd",
#    "hold_kz",
#    "hold_kn",
#    "hold_kpd",
#    "hold_kpers",
#    "hold_ks",
#    "hold_nds",
#    "hold_kf",
#    "hold_kfapi",
#    "hold_fms",
#    "hold_ke",
#    "hold_kemb",
#    "hold_kep",
#    "hold_rpn",
#    "hold_seminar",
#    "hold_fst",
#    "hold_shb",
#    "hold_shet",
#    "hold_etp",
#    "hold_all",
    "fix_edi",
    "fix_adk",
    "fix_amba",
    "fix_asv",
    "fix_dd",
    "fix_infbez",
    "fix_kb",
    "fix_kba",
    "fix_kbb",
    "fix_kd",
    "fix_kz",
    "fix_kn",
    "fix_kpd",
    "fix_kpers",
    "fix_ks",
    "fix_nds",
    "fix_kf",
    "fix_kfapi",
    "fix_fms",
    "fix_ke",
    "fix_kemb",
    "fix_kep",
    "fix_rpn",
    "fix_seminar",
    "fix_fst",
    "fix_shb",
    "fix_shet",
    "fix_etp",
    "fix_all",
    "ksnds_date_last_login",
    "is_load_book",
#    "cnt_inviters",
#    "ksnds_activity",
    "cnt_demand_3m",# collect into chanks
    "cnt_ca_8",# collect into chanks
    "cnt_ca_8_active_ca",# collect into chanks
    "cnt_ca_9",# collect into chanks
    "cnt_ca_9_active_ca",# collect into chanks
#    "proc_ca_8",
#    "proc_ca_9",
    "invoices_count_8",# collect into chanks
    "invoices_count_9",# collect into chanks
#    "invoices_diff_count_8",
#    "invoices_diff_count_9",
#    "invoices_diff_nds_8",
#    "invoices_diff_nds_9",
#    "invoices_diff_nds_8_range",
    "order_new_ke",
    "order_new_kep",
    "order_new_bk",
    "order_new_dd",
    "order_new_ka",
    "order_new_kz",
    "order_new_nds",
    "order_new_kn",
    "order_new_kf",
    "order_new_etp",
    "is2DocsOpened",
    "is2TimesVisited",
    "isSubscribed",
    "isTurboActivated",
    "is2TimesSearched"
]
# Columns to convert dates
columns_to_convert_dates = ["reg_dt",
                            "since_asv_max",
                            "upto_asv_max",
                            "since_asv_min",
                            "upto_asv_min",
                            "since_bk",
                            "upto_bk",
                            "since_diadoc",
                            "upto_diadoc",
                            "since_edi",
                            "upto_edi",
                            "since_alko",
                            "upto_alko",
                            "since_egais",
                            "upto_egais",
                            "since_kz",
                            "upto_kz",
                            "since_fms",
                            "upto_fms",
                            "since_kn",
                            "upto_kn",
                            "since_kf",
                            "upto_kf",
                            "since_ke",
                            "upto_ke",
                            "since_kep",
                            "upto_kep",
                            "since_kep_egais",
                            "upto_kep_egais",
                            "since_nds",
                            "upto_nds",
                            "since_etp",
                            "upto_etp",
                            "since_kemb",
                            "upto_kemb",
                            "demo_kopf_date",
                            "etp_activity_bd",
                            "etp_activity_ed",
                            "etp_accredit_bd",
                            "etp_accredit_ed",
                            "cert_etprf_upto",
                            "cert_sb_upto",
                            "ksnds_date_last_login"
                            ]
              
# Columns to categorise 
columns_to_categorize = [
    # column, max value, category step
    ["personal",0,100000,100], # collect into chanks
    ["cnt_founder_ul",0,100, 20], # collect into chanks
    ["cnt_founder_fl",0,500, 50], # collect into chanks
    ["revenue",0,2000000000000, 1000000],
    ["revenue2014",0,2000000000000, 1000000], # collect into chanks
    ["revenue2015",0,2000000000000, 1000000], # collect into chanks
    ["profit2014",-50000000000,50000000000,1000000], # collect into chanks
    ["arbitrage_c",0,10000,10], # collect into chanks
    ["arbitrage_c_lastyear",0,10000,10], # collect into chanks
    ["arbitrage_d",0,10000,10],# collect into chanks
    ["arbitrage_cd",0,10000,10],# collect into chanks
    ["etp_cnt",0,50000,1000],# collect into chanks
    ["alko_shipper_cnt",0,50,10],# collect into chanks
    ["summ_contracts44",0,50000000000,1000000],# collect into chanks
    ["summ_contracts223",0,50000000000,1000000],# collect into chanks
    ["cnt_ca_8",0,1000,10],# collect into chanks
    ["cnt_ca_8_active_ca",0,1000,10],# collect into chanks
    ["cnt_ca_9",0,1000,10],# collect into chanks
    ["cnt_ca_9_active_ca",0,1000,10],# collect into chanks
    ["invoices_count_8",0,1000000,10000],# collect into chanks
    ["invoices_count_9",0,1000000,10000]# collect into chanks
]


# Haming Distance
def haming_distance_lists (row1, row2):
    sim = 0
    for el in range(0,len(row1)):
        #print (row1[el]," == ",row2[el])
        if row1[el] != None and row2[el] != None and row1[el] == row2[el]:
            #print ("Yes")
            sim = sim +1
    #print("Dist btw {} and {} = {}".format(row1[0],row2[0],len(row1) - sim))
    return len(row1) - sim



# Assign to clusters MR    
def assign_cluster_map (row_id,row,clusters):
    min_distance = len(row)
    min_clusteroid = -1
    for cluster in clusters:
        dist = haming_distance_lists (cluster[1], row)
        if dist < min_distance:
            min_distance = dist
            min_clusteroid = cluster 
    #print ("({}->{}:{}) ".format(row_id,min_clusteroid[0],min_distance)),
    return (min_clusteroid[0],min_distance)


# Recalculating clusteroids MR   
def recalculate_clusteroids_mp(clusters,data_rdd_rev_assigned_clusters):
    new_clusteroids = []
    data_rdd_rev_assigned_clusters = sc.parallelize(data_rdd_rev_assigned_clusters)
    # for each clusteroid:
    for cluster in clusters:
        #print "=========="
        #print "cluster ",cluster[0]
        # 1 Get all dots in cluster.
        # 2 Get distances to clusteroid. Take N closest.
        cluster_to_recalculate = sc.parallelize(data_rdd_rev_assigned_clusters.\
            filter(lambda (x,y,z): (z[0]==cluster[0])).\
            map(lambda (x,y,z):(x,y,z)).\
            sortBy(lambda (x,y,z):z[1]).\
            take(points_to_learn))
    
        min_distance = cluster_to_recalculate.map(lambda (x,y,z): z[1]).\
            reduce(lambda accum, n: accum + n)

        min_clusteroid = cluster
        #print "min_distance ",min_distance
    
        sum_clusteroid = cluster
        for el in cluster_to_recalculate.collect():
            new_cluster = cluster_to_recalculate.\
                map(lambda (x,y,z): (el[0], haming_distance_lists(el[1],y) )).\
                reduceByKey(add).\
                collect()
            #print "count for ",el[0], "dist for ",new_cluster[0][0],"is",new_cluster[0][1]
            if new_cluster[0][1] < min_distance:
                min_distance = new_cluster[0][1]
                min_clusteroid = el
                print ("Cluster {} new clusteroid {} min summ distance {}".format(cluster[0],min_clusteroid[0],min_distance))
        
        new_clusteroids.append((min_clusteroid[0],min_clusteroid[1],min_distance))
        #print ("----New clusteroids {}".format(new_clusteroids))
    return new_clusteroids


#### Driver Program

# Data preparation
print ""
print "---------"
print "Clasterisation - k-maens - beginning"
print "Finde Clusteroids"

print "Read from file: ",file_name
#Read Data
print"Read Data",
t0 = time()

data_rdd = sqlContext.read.format("csv").options(header="true",charset="utf-8").load(file_name).rdd
data_rdd.collect()
# Add indexes
data_rdd = data_rdd.zipWithIndex()
data_rdd.collect()
# Put Indexes first
data_rdd_rev = data_rdd.map(lambda (x,y): (y,x))

tt = time() - t0
print " completed in {} seconds".format(round(tt,3))


#Random clusters
print"Get random clusteroids",
t0 = time()
new_clusteroids_array = data_rdd_rev.takeSample(False,number_of_clusteroids)
tt = time() - t0
print " completed in {} seconds".format(round(tt,3))
print "Rundom clusteroids {}".format(sc.parallelize(new_clusteroids_array).map(lambda (x,y):x).collect())
    

# Recalculate clusteroid:
clusteroids_array = []
step = 1
print "Start k-means"
while (clusteroids_array != new_clusteroids_array):
    clusteroids_array = list(new_clusteroids_array)
    print "---------"
    print "Run N {}".format(step)
    t0_run = time()

    #Cluster Assignment
    print"Cluster Assignment",
    t0 = time()
    data_rdd_rev_assigned_clusters = data_rdd_rev.map(lambda (x,y): (x,y,assign_cluster_map(x,y,clusteroids_array))).collect()
    tt = time() - t0
    print " completed in {} seconds".format(round(tt,3))

    #Recalculate Clusteroids
    print"Recalculate Clusteroids"
    t0 = time()
    new_clusteroids_array = recalculate_clusteroids_mp(clusteroids_array, data_rdd_rev_assigned_clusters)
    tt = time() - t0
    print " completed in {} seconds".format(round(tt,3))
    #new_clusteroids_array = new_clusteroids_with_density[0]
    #new_clusters_density = new_clusteroids_with_density[1]
    #print "Run {} Intermidiate clusteroids old {}".format(step,sc.parallelize(clusteroids_array).map(lambda (x,y,z):x).collect())
    #print "Run {} Intermidiate clusters {}".format(step,clusteroids_array)
    print "Run {} Intermidiate clusteroids new {}".format(step,sc.parallelize(new_clusteroids_array).map(lambda (x,y,z):(x,z)).collect())
    tt_run = time() - t0_run
    print "Run N {} completed in {} seconds".format(step,round(tt_run,3))
    step = step + 1

print "Clasterisation - end."

print "{} runs, final clusteroids with local density {}".format(step,sc.parallelize(new_clusteroids_array).\
    map(lambda (x,y,z):(x,z)).\
    collect())

#print "Final cluster density {}".format(new_clusters_density)
print ""










from pyspark.sql.types import StructType

schema = StructType.fromJson(
{'fields': [
    {'metadata': {},'name': 'primaryid', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'caseid', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'caseversion', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'i_f_code', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'i_f_code_num', 'nullable': True,'type': 'integer'},
  {'metadata': {}, 'name': 'event_dt', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'event_dt_num', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'mfr_dt', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'mfr_dt_num', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'init_fda_dt', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'init_fda_dt_num','nullable': True,'type': 'string'},
  {'metadata': {}, 'name': 'fda_dt', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'fda_dt_num', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'rept_cod', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'rept_cod_num','nullable': True,'type': 'integer'},
  {'metadata': {}, 'name': 'auth_num', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'mfr_num', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'mfr_sndr', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'lit_ref', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'age', 'nullable': True, 'type': 'double'},
  {'metadata': {}, 'name': 'age_cod', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'age_grp', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'age_grp_num', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'sex', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'e_sub', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'wt', 'nullable': True, 'type': 'double'},
  {'metadata': {}, 'name': 'wt_cod', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'rept_dt', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'rept_dt_num', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'to_mfr', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'occp_cod', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'reporter_country', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'occr_country', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'occp_cod_num', 'nullable': True, 'type': 'integer'}
],'type': 'struct'})

#id_org	is_head	is_client	is_ke_abonent	is_ke_head	ogrn	stts	okved	reg_dt	region_code	cptl


# In[ ]:

#(spark.read.schema(schema).format("csv").options(header="true").load("/path/to/demo2016q1.csv"))

