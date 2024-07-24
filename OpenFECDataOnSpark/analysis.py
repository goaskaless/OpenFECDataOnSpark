indivs24_s3 = f"s3://fec2024/indiv24/itcont_new.txt"
pacstocands_s3 = f"s3://fec2024/indiv24/itpas2.txt"
cmtemaster_s3 = f"s3://fec2024/indiv24/cm.txt"
candsmaster_s3 = f"s3://fec2024/indiv24/cn.txt"
cmtetocand_s3 = f"s3://fec2024/indiv24/ccl.txt"
pacsmaster_s3 = f"s3://fec2024/indiv24/webk24.txt"
pactopac_s3 = f"s3://fec2024/indiv24/itoth.txt.txt"

sc = spark.sparkContext
sqlContext = HiveContext(sc)

indivs_df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false', delimiter='|') \
    .load(f"{indivs24_s3}")

pacstocands_df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false', delimiter='|') \
    .load(f"{pacsmaster_s3}")

cmtemaster_df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false', delimiter='|') \
    .load(f"{cmtemaster_s3}")

candsmaster_df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false', delimiter='|') \
    .load(f"{candsmaster_s3}")

cmtetocand_df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false', delimiter='|') \
    .load(f"{cmtetocand_s3}")

pacsmaster_df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false', delimiter='|') \
    .load(f"{pacsmaster_s3}")

pactopac_df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false', delimiter='|') \
    .load(f"{pactopac_s3}")

indivs_df = indivs_df.selectExpr( "_c0 as CMTE_ID",  "_c1 as AMNDT_IND" ,  "_c2 as RPT_TP" ,  "_c3 as TRANSACTION_PGI" ,  "_c4 as IMAGE_NUM" ,  "_c5 as TRANSACTION_TP" ,  "_c6 as ENTITY_TP" ,  "_c7 as NAME" ,  "_c8 as CITY" ,  "_c9 as STATE" ,  "_c10 as ZIP_CODE" ,  "_c11 as EMPLOYER" ,  "_c12 as OCCUPATION" ,  "_c13 as TRANSACTION_DT" ,  "_c14 as TRANSACTION_AMT" ,  "_c15 as OTHER_ID" ,  "_c16 as TRAN_ID" ,  "_c17 as FILE_NUM" ,  "_c18 as MEMO_CD" ,  "_c19 as MEMO_TEXT" ,  "_c20 as SUB_ID" ) 
pacstocands_df = pacstocands_df.selectExpr( "_c0 as CMTE_ID" ,  "_c1 as AMNDT_IND" ,  "_c2 as RPT_TP" ,  "_c3 as TRANSACTION_PGI" ,  "_c4 as IMAGE_NUM" ,  "_c5 as TRANSACTION_TP" ,  "_c6 as ENTITY_TP" ,  "_c7 as NAME" ,  "_c8 as CITY" ,  "_c9 as STATE" ,  "_c10 as ZIP_CODE" ,  "_c11 as EMPLOYER" ,  "_c12 as OCCUPATION",   "_c13 as TRANSACTION_DT" ,  "_c14 as TRANSACTION_AMT" ,  "_c15 as OTHER_ID" , "_c16 as CAND_ID" ,  "_c17 as TRAN_ID" ,  "_c18 as FILE_NUM" ,  "_c19 as MEMO_CD" ,  "_c20 as MEMO_TEXT" ,  "_c21 as SUB_ID" ) 
cmtemaster_df = cmtemaster_df.selectExpr( "_c0 as CMTE_ID" ,  "_c1 as CMTE_NM" ,  "_c2 as TRES_NM" ,  "_c3 as CMTE_ST1" ,  "_c4 as CMTE_ST2" ,  "_c5 as CMTE_CITY" ,  "_c6 as CMTE_ST" ,  "_c7 as CMTE_ZIP" ,  "_c8 as CMTE_DSGN" ,  "_c9 as CMTE_TP" ,  "_c10 as CMTE_PTY_AFFILIATION" ,  "_c11 as CMTE_FILING_FREQ" ,  "_c12 as ORG_TP" ,  "_c13 as CONNECTED_ORG_NM" ,  "_c14 as CAND_ID" ) 
candsmaster_df = candsmaster_df.selectExpr( "_c0 as CAND_ID" ,  "_c1 as CAND_NAME" ,  "_c2 as CAND_PTY_AFFILIATION" ,  "_c3 as CAND_ELECTION_YR" ,  "_c4 as CAND_OFFICE_ST" ,  "_c5 as CAND_OFFICE" ,  "_c6 as CAND_OFFICE_DISTRICT" ,  "_c7 as CAND_ICI" ,  "_c8 as CAND_STATUS" ,  "_c9 as CAND_PCC" ,  "_c10 as CAND_ST1" ,  "_c11 as CAND_ST2" ,  "_c12 as CAND_CITY" ,  "_c13 as CAND_ST" ,  "_c14 as CAND_ZIP" )    
cmtetocand_df = cmtetocand_df.selectExpr( "_c0 as CAND_ID" ,  "_c1 as CAND_ELECTION_YR" ,  "_c2 as FEC_ELECTION_YR" ,  "_c3 as CMTE_ID" , "_c4 as CMTE_TP", "_c5 as CMTE_DSGN", "_c6 as LINKAGE_ID")
pacsmaster_df = pacsmaster_df.selectExpr( "_c0 as CMTE_ID" ,  "_c1 as CMTE_NM" ,  "_c2 as CMTE_TP" ,  "_c3 as CMTE_DSGN" ,  "_c4 as CMTE_FILING_FREQ" ,  "_c5 as TTL_RECEIPTS" ,  "_c6 as TRANS_FROM_AFF" ,  "_c7 as INDV_CONTRIB" ,  "_c8 as OTHER_POL_CMTE_CONTRIB" ,  "_c9 as CAND_CONTRIB" ,  "_c10 as CAND_LOANS" ,  "_c11 as TTL_LOANS_RECEIVED" ,  "_c12 as TTL_DISB" ,  "_c13 as TRANF_TO_AFF" ,  "_c14 as INDV_REFUNDS", "_c15 as OTHER_POL_CMTE_REFUNDS", "_c16 as CAND_LOAN_REPAY", "_c17 as LOAN_REPAY", "_c18 as COH_BOP", "_c19 as COH_COP", "_c20 as DEBTS_OWED_BY", "_c21 as NONFED_TRANS_RECEIVED", "_c22 as CONTRIB_TO_OTHER_CMTE", "_c23 as IND_EXP", "_c24 as PTY_COORD_EXP", "_c25 as NONFED_SHARE_EXP", "_c26 as CVG_END_DT")
pactopac_df = pactopac_df.selectExpr("_c0 as CMTE_ID" ,	"_c1 as AMNDT_IND"	, "_c2 as RPT_TP"	, "_c3  as TRANSACTION_PGI" ,	"_c4 as IMAGE_NUM" ,  "_c5 as TRANSACTION_TP" ,  "_c6 as ENTITY_TP" ,  "_c7 as NAME" ,  "_c8 as CITY" ,  "_c9 as STATE" ,  "_c10 as ZIP_CODE" ,  "_c11 as EMPLOYER" ,  "_c12 as OCCUPATION" ,  "_c13 as TRANSACTION_DT" ,  "_c14 as TRANSACTION_AMT", "_c15 as OTHER_ID", "_c16 as TRAN_ID", "_c17 as FILE_NUM", "_c18 as MEMO_CD", "_c19 as MEMO_TEXT", "_c20 as SUB_ID")

indivs_df.createOrReplaceTempView("indivs")
pacstocands_df.createOrReplaceTempView("pactocand")
cmtemaster_df.createOrReplaceTempView("cmteref")
candsmaster_df.createOrReplaceTempView("candsref")
cmtetocand_df.createOrReplaceTempView("connect")
pacsmaster_df.createOrReplaceTempView("pacref")
pactopac_df.createOrReplaceTempView("pactopac")

#question 1 individual presidential fundraising total
indivs_presidential_war_chest = spark.sql("SELECT SUM(a.TRANSACTION_AMT) as sum_contrib, AVG(a.TRANSACTION_AMT) as average_contrib, c.CAND_NAME, d.CMTE_NM from indivs a \
        inner join connect b on a.CMTE_ID = b.CMTE_ID \
        inner join candsref c on b.CAND_ID = c.CAND_ID \
        left join pacref d on a.CMTE_ID = d.CMTE_ID \
        where c.CAND_ELECTION_YR = '2024' and (c.CAND_NAME like ('%TRUMP%') or c.CAND_NAME like ('%BIDEN%') or c.CAND_NAME like ('%ROBERT F JR%'))\
        group by c.CAND_NAME, d.CMTE_NM order by SUM(a.TRANSACTION_AMT) DESC")

indivs_presidential_war_chest.coalesce(1).write.csv('s3a://fec2024/outputfiles/indivs_presidential_war_chest.csv', mode='overwrite', header='true')

#question 2 pac to cmte transfer presidential fundraising total
pacs_to_pres_war_chest_2024 = spark.sql("SELECT SUM(a.TRANSACTION_AMT) as sum_contrib, AVG(a.TRANSACTION_AMT) as average_contrib, b.CMTE_NM AS TFER_TO, e.CMTE_NM as TFER_FROM, d.CAND_NAME, d.CAND_PTY_AFFILIATION from pactopac a \
          inner join pacref b on a.CMTE_ID = b.CMTE_ID \
          inner join pacref e on a.OTHER_ID = e.CMTE_ID \
          inner join connect c on a.CMTE_ID = c.CMTE_ID \
          inner join candsref d on c.CAND_ID = d.CAND_ID \
          where d.CAND_ELECTION_YR = '2024' \
          and (d.CAND_NAME like ('%TRUMP%') or d.CAND_NAME like ('%BIDEN%') or d.CAND_NAME like ('%ROBERT F JR%'))\
          group by b.CMTE_NM, e.CMTE_NM, d.CAND_NAME, d.CAND_PTY_AFFILIATION \
          order by SUM(a.TRANSACTION_AMT) desc")

pacs_to_pres_war_chest_2024.coalesce(1).write.csv('s3a://fec2024/outputfiles/pacs_to_pres_war_chest_2024.csv', mode='overwrite', header='true')

#question 3 individual contribution presendential fundraising by states
presidential_war_chest_by_state = spark.sql("SELECT SUM(a.TRANSACTION_AMT) as sum_contrib, AVG(a.TRANSACTION_AMT) as average_contrib, a.STATE, c.CAND_NAME from indivs a \
        inner join connect b on a.CMTE_ID = b.CMTE_ID \
        inner join candsref c on b.CAND_ID = c.CAND_ID \
        where c.CAND_NAME like ('%TRUMP%') or c.CAND_NAME like ('%BIDEN%') or c.CAND_NAME like ('%ROBERT F JR%')\
        group by c.CAND_NAME, a.STATE order by SUM(a.TRANSACTION_AMT) DESC")

presidential_war_chest_by_state.coalesce(1).write.csv('s3a://fec2024/outputfiles/presidential_war_chest_by_state.csv', mode='overwrite', header='true')

#question 4 individual presidential fundraising by occupation
presidential_war_chest_by_occupation = spark.sql("SELECT SUM(a.TRANSACTION_AMT) as sum_contrib, AVG(a.TRANSACTION_AMT) as average_contrib, a.OCCUPATION, c.CAND_NAME from indivs a \
        inner join connect b on a.CMTE_ID = b.CMTE_ID \
        inner join candsref c on b.CAND_ID = c.CAND_ID \
        where c.CAND_ELECTION_YR = '2024' \
        and c.CAND_NAME like ('%TRUMP%') or c.CAND_NAME like ('%BIDEN%') or c.CAND_NAME like ('%ROBERT F JR%')\
        group by c.CAND_NAME, a.OCCUPATION order by SUM(a.TRANSACTION_AMT) DESC")

presidential_war_chest_by_occupation.coalesce(1).write.csv('s3a://fec2024/outputfiles/presidential_war_chest_by_occupation.csv', mode='overwrite', header='true')

#question 5 aggregate by state extent of D vs R contributions
indiv_party_contributions_by_state = spark.sql("SELECT SUM(a.TRANSACTION_AMT) as sum_contrib, AVG(a.TRANSACTION_AMT) as average_contrib, a.STATE, c.CAND_PTY_AFFILIATION from indivs a \
          inner join connect b on a.CMTE_ID = b.CMTE_ID \
          inner join candsref c on b.CAND_ID = c.CAND_ID \
          where c.CAND_PTY_AFFILIATION is not null and c.CAND_ELECTION_YR = '2024' \
          group by a.STATE, c.CAND_PTY_AFFILIATION")

indiv_party_contributions_by_state.coalesce(1).write.csv('s3a://fec2024/outputfiles/indiv_party_contributions_by_state.csv', mode='overwrite', header='true')