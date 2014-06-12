import pandas as pd, numpy as np
import psycopg2
import pandas.io.sql as sql

def get_pums():
    conn_string = "host='paris.urbansim.org' dbname='denver' user='drcog' password='M0untains#' port=5433"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    pums_hh = sql.read_frame('select * from pums_hh',conn)
    pums_p = sql.read_frame('select * from pums_person',conn)
    pums_hh = pums_hh.set_index('serialno')
    pums_p = pums_p.set_index('serialno')
    pums_hh = pums_hh[(pums_hh.type==1)*(pums_hh.np>0)]
    pums_p.agep[(pums_p.relp==0)*(pums_p.agep<16)] = 16
    pums_p['age'] = pums_p.agep
    pums_p['relate'] = pums_p.relp
    pums_p['esr_recode'] = 0*(pums_p.esr.isin([-99,6])) + 1*(pums_p.esr.isin([1,4])) + 2*(pums_p.esr.isin([2,5])) + 3*(pums_p.esr==3)
    pums_p['grade'] = pums_p.schg*(pums_p.schg>0) + 0*(pums_p.schg==-99)
    pums_p['pnum'] = pums_p.sporder
    pums_p['paug'] = 0
    pums_p['ddp'] = 0
    pums_p['sex'] = pums_p.sex
    pums_p['weeks'] = 0*(pums_p.wkw==-99) + 7*(pums_p.wkw==6) + 20*(pums_p.wkw==5) + 33*(pums_p.wkw==4) + 44*(pums_p.wkw==3) + 49*(pums_p.wkw==2) + 52*(pums_p.wkw==1)
    pums_p['hours'] = pums_p.wkhp*(pums_p.wkhp>0) + 0*(pums_p.wkhp==-99)
    pums_p['race1'] = pums_p.rac1p
    pums_p['hispan'] = pums_p.hisp
    pums_p['msp'] = pums_p.msp ##Get rid of -99's in the whole dataframe at end
    pums_p['poverty'] = pums_p.povpip
    pums_hh.adjinc = pums_hh.adjinc*.000001
    pums_p['inc_adjustment'] = pums_hh.adjinc[pums_p.index.values].values
    pums_p['earns'] = pums_p.pernp*pums_p.inc_adjustment*.74 ##Adjustment from 2011 dollars to 1999 dollars
    # pums_p.indp07[pums_p.indp07=='N.A.'] = '0'
    # pums_p.indp07[pums_p.indp07==''] = '0'
    # pums_p['indnaics'] = pums_p.indp07.astype('float').astype('int32')
    pums_p.naicsp07[pums_p.naicsp07=='N.A.////'] = '0'
    pums_p.naicsp07[pums_p.naicsp07==''] = '0'
    pums_p.naicsp07 = pums_p.naicsp07.str.slice(0,3)
    pums_p.naicsp07[pums_p.naicsp07=='22S'] = '221'
    pums_p.naicsp07[pums_p.naicsp07=='23'] = '230'
    pums_p.naicsp07[pums_p.naicsp07=='31M'] = '313'
    pums_p.naicsp07[pums_p.naicsp07=='33M'] = '331'
    pums_p.naicsp07[pums_p.naicsp07=='3MS'] = '311'
    pums_p.naicsp07[pums_p.naicsp07=='42S'] = '423'
    pums_p.naicsp07[pums_p.naicsp07=='4M'] = '443'
    pums_p.naicsp07[pums_p.naicsp07=='4MS'] = '443'
    pums_p.naicsp07[pums_p.naicsp07=='52M'] = '522'
    pums_p.naicsp07[pums_p.naicsp07=='53M'] = '532'
    pums_p.naicsp07[pums_p.naicsp07=='55'] = '561'
    pums_p.naicsp07[pums_p.naicsp07=='92M'] = '921'
    pums_p['indnaics'] = pums_p.naicsp07.astype('float').astype('int32')
    pums_p['pemploy'] = 1*(pums_p.wkhp>=25) + 2*(pums_p.wkhp<25)*(pums_p.wkhp>0) + 3*(pums_p.wkhp<1)
    pums_p['pstudent'] = 1*(pums_p.schg<=5)*(pums_p.schg>0) + 2*(pums_p.schg>5) + 3*(pums_p.schg==-99)
    pums_p['padkid'] = 1*(pums_p.agep>=18)*(pums_p.agep<=24)*(pums_p.relp.isin([1,2,4,5]))
    pums_p.padkid[pums_p.padkid==0] = 2
    pums_p['pagecat'] = 0*(pums_p.agep<=5) + 1*(pums_p.agep>=6)*(pums_p.agep<=11) + 2*(pums_p.agep>=12)*(pums_p.agep<=15) + 3*(pums_p.agep>=16)*(pums_p.agep<=17) \
                        + 4*(pums_p.agep>=18)*(pums_p.agep<=24) + 5*(pums_p.agep>=25)*(pums_p.agep<=34) + 6*(pums_p.agep>=35)*(pums_p.agep<=49) \
                        + 7*(pums_p.agep>=50)*(pums_p.agep<=64) + 8*(pums_p.agep>=65)*(pums_p.agep<=79) + 9*(pums_p.agep>=80)
    pums_p['ptype'] = 1*(pums_p.agep>=16)*(pums_p.wkhp>=25)*(pums_p.schg==-99) + 2*(pums_p.agep>=16)*(pums_p.wkhp>0)*(pums_p.wkhp<25)*(pums_p.schg==-99) + 3*(pums_p.schg>5) \
                      + 4*(pums_p.schg==-99)*(pums_p.wkhp==-99)*(pums_p.agep>=16)*(pums_p.agep<=64) + 5*(pums_p.schg==-99)*(pums_p.wkhp==-99)*(pums_p.agep>=65) \
                      + 6*(pums_p.schg<6)*(pums_p.wkhp<25)*(pums_p.agep>=16)*(pums_p.agep<=19) + 7*(pums_p.agep>=6)*(pums_p.agep<=15) + 8*(pums_p.agep<=5)  
            
    joined = pd.merge(pums_p, pums_hh, left_index=True, right_index=True)
    joined['serialno'] = joined.index.values
    joined['is_worker'] = np.in1d(joined.esr,[1,2,3,4,5]).astype('int32')

    num_children = joined[joined.agep<18].groupby('serialno').size()
    num_adults = joined[joined.agep>=18].groupby('serialno').size()
    num_workers = joined.groupby('serialno').is_worker.sum()

    pums_hh['age_of_head'] = joined[joined.relp==0].agep
    pums_hh['children'] = num_children

    pums_hh['hhagecat'] = 1*(pums_hh.age_of_head<65) + 2*(pums_hh.age_of_head>=65)
    pums_hh['hhagecat3'] = 1*(pums_hh.age_of_head<45) + 2*(pums_hh.age_of_head>=45)*(pums_hh.age_of_head<65) + 3*(pums_hh.age_of_head>=65)

    pums_hh['hsizecat'] = pums_hh.np*(pums_hh.np<=5) + 5*(pums_hh.np>5)
    pums_hh['hsizecat6'] = pums_hh.np*(pums_hh.np<=6) + 6*(pums_hh.np>6)
    pums_hh['hfamily'] = 1*(pums_hh.hht>3) + 2*(pums_hh.hht<=3)
    pums_hh['hunittype'] = 0
    pums_hh['hnoccat'] = 0*(pums_hh.noc==0) + 1*(pums_hh.noc>0)
    pums_hh['hnccat6'] = num_children*(num_children<5) + 5*(num_children>=5)
    pums_hh['hnacat6'] =  num_adults*(num_adults<5) + 5*(num_adults>=5)
    pums_hh['hwrkrcat'] = num_workers*(num_workers<3) + 3*(num_workers>=3)
    pums_hh['hwrkcat5'] = num_workers*(num_workers<4) + 4*(num_workers>=4)
    pums_hh['h0005'] = joined[joined.agep<=5].groupby('serialno').size()
    pums_hh['h0611'] = joined[(joined.agep>=6)*(joined.agep<=11)].groupby('serialno').size()
    pums_hh['h1215'] = joined[(joined.agep>=12)*(joined.agep<=15)].groupby('serialno').size()
    pums_hh['h1617'] = joined[(joined.agep>=16)*(joined.agep<=17)].groupby('serialno').size()
    pums_hh['h1824'] = joined[(joined.agep>=18)*(joined.agep<=24)].groupby('serialno').size()
    pums_hh['h2534'] = joined[(joined.agep>=25)*(joined.agep<=34)].groupby('serialno').size()
    pums_hh['h3549'] = joined[(joined.agep>=35)*(joined.agep<=49)].groupby('serialno').size()
    pums_hh['h5064'] = joined[(joined.agep>=50)*(joined.agep<=64)].groupby('serialno').size()
    pums_hh['h6579'] = joined[(joined.agep>=65)*(joined.agep<=79)].groupby('serialno').size()
    pums_hh['h80up'] = joined[(joined.agep>=80)].groupby('serialno').size()
    pums_hh['hworkers'] = num_workers
    pums_hh['hwork_f'] = joined[joined.ptype==1].groupby('serialno').size()
    pums_hh['hwork_p'] = joined[joined.ptype==2].groupby('serialno').size()
    pums_hh['huniv'] = joined[joined.ptype==3].groupby('serialno').size()
    pums_hh['hnwork'] = joined[joined.ptype==4].groupby('serialno').size()
    pums_hh['hretire'] = joined[joined.ptype==5].groupby('serialno').size()
    pums_hh['hpresch'] = joined[joined.ptype==8].groupby('serialno').size()
    pums_hh['hschpred'] = joined[joined.ptype==7].groupby('serialno').size()
    pums_hh['hschdriv'] = joined[joined.ptype==6].groupby('serialno').size()
    pums_hh['hadnwst'] = joined[(joined.agep>=16)*(joined.wkhp==-99)*(joined.schg>0)].groupby('serialno').size()
    pums_hh['hadwpst'] = joined[(joined.agep>=16)*(joined.wkhp>0)*(joined.schg>0)].groupby('serialno').size()
    pums_hh['hadkids'] = joined[(joined.agep>=18)*(joined.relp==2)].groupby('serialno').size()

    pums_hh['puma5'] = pums_hh.puma  ##Needs to be updated by urbansim!!
    pums_hh['hinc'] = pums_hh.hincp*pums_hh.adjinc*.74 ##Adjustment from 2011 dollars to 1999 dollars
    pums_hh['persons'] = pums_hh.np
    pums_hh.hht[pums_hh.hht==-99] = 0
    pums_hh['unittype'] = pums_hh.type - 1
    pums_hh['hinccat1'] = 1*(pums_hh.hinc<20000) + 2*(pums_hh.hinc>=20000)*(pums_hh.hinc<50000) + 3*(pums_hh.hinc>=50000)*(pums_hh.hinc<100000) + 4*(pums_hh.hinc>=100000)
    pums_hh['hinccat2'] = 1*(pums_hh.hinc<10000) + 2*(pums_hh.hinc>=10000)*(pums_hh.hinc<20000) + 3*(pums_hh.hinc>=20000)*(pums_hh.hinc<30000) + \
                          4*(pums_hh.hinc>=30000)*(pums_hh.hinc<40000) + 5*(pums_hh.hinc>=40000)*(pums_hh.hinc<50000) + 6*(pums_hh.hinc>=50000)*(pums_hh.hinc<60000) + \
                          7*(pums_hh.hinc>=60000)*(pums_hh.hinc<75000) + 8*(pums_hh.hinc>=75000)*(pums_hh.hinc<100000) +  9*(pums_hh.hinc>=100000)
    pums_hh['hownrent'] = 1*(pums_hh.ten<3) + 2*(pums_hh.ten>2)
    pums_hh['bucketBin'] = 0
    pums_hh['originalpuma'] = pums_hh.puma
    for col in pums_hh:
        if col not in ['adjinc','hinc']:
            if pums_hh[col].dtype== np.float:
                pums_hh[col] = pums_hh[col].fillna(0).astype('int32')
                
    return pums_hh, pums_p