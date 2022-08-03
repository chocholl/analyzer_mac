import sys
import pandas as pd
import ast
import copy
import os


def load_mapping(c_file_src_name):
    map_dict = {}
    with open(c_file_src_name, "r") as f:
        for line in f:
            #(key, val) = line.split(' ')
            spl = line.split(' ')
            #map_dict[spl[0]] = spl[1].rstrip()
            map_dict[spl[0].lower()] = spl[1].rstrip().lower()
    return map_dict

def get_result(p_f_name, is_screen=False):
   ret = []
   keys = []
   
   f = open(p_f_name)
   i = 0
   for line in f:
      l_line = line.rstrip()
      if(i==0):
         keys = l_line.split(';')
         i=i+1
      else:
         j=0
         d = {}
         for val in l_line.split(';'):
            if(val!=''):
               d[keys[j]]=val
            j=j+1
         ret.append(d)
   f.close()

   if( is_screen == True ):
      print ret

   return ret

def print_result(p_f_name, p_keys_list, p_dict_list, is_screen=False):

   all_keys = []
   for t_s in p_dict_list:
      keys = t_s.keys()
      for k in keys:
         if ((k not in all_keys) and (k not in p_keys_list)):
            all_keys.append(k)
   if(len(all_keys)>0):
      all_keys.sort()

   f= open(p_f_name,"w")
   out=''
   p = p_keys_list + all_keys

   for pp in p:
      out = out + pp + ';'
   out = out[:-1]
   if (is_screen==True):
      print out
   f.write(out+"\n")

   for ss in p_dict_list:
      out = ''
      for pp in p:
         out = out + str(ss.get(pp, '')) + ';'
      out = out[:-1]
      if (is_screen==True):
         print out
      f.write(out+"\n")

   f.close() 




params = load_mapping('params.txt')
a_list = []
b_list = []


a_list = ast.literal_eval(params['a_list'])
b_list = ast.literal_eval(params['b_list'])


mac1_list = []
mac2_list = []

if(True):
   for s in a_list:
      
      c_mac = get_result(s)   
      for m1 in c_mac:
         found = False
         for m2 in mac1_list:         
            if(m1['mac'] == m2['mac'] and m1['vlan'] == m2['vlan']):
               found = True
               break
         if(found == False):
            m1_copy = copy.deepcopy(m1)
            mac1_list.append(m1_copy)

   for s in b_list:
      c_mac = get_result(s)   
      for m1 in c_mac:
         found = False
         for m2 in mac2_list:         
            if(m1['mac'] == m2['mac'] and m1['vlan'] == m2['vlan']):
               found = True
               break
         if(found == False):
            m1_copy = copy.deepcopy(m1)
            mac2_list.append(m1_copy)

   print_result("_mac1.csv", [], mac1_list)
   print_result("_mac2.csv", [], mac2_list)


df1 = pd.read_csv("_mac1.csv", sep=';')
if('age' in df1.columns):
   df1.drop(['age'], axis='columns', inplace=True)

df2 = pd.read_csv("_mac2.csv", sep=';')
if('age' in df2.columns):
   df2.drop(['age'], axis='columns', inplace=True)





#######################

mac_count_1 = df1.groupby('port')['mac'].count()
mac_count_2 = df2.groupby('port')['mac'].count()
mac_count_per_port = pd.DataFrame(mac_count_1).merge(pd.DataFrame(mac_count_2), how='outer', on='port' )
mac_count_per_port.to_csv('_mac_count_per_port.csv',  sep=';', header=['mac_count_1', 'mac_count_2'])


mac_count_1 = df1.groupby('vlan')['mac'].count()
mac_count_2 = df2.groupby('vlan')['mac'].count()
mac_count_per_vlan = pd.DataFrame(mac_count_1).merge(pd.DataFrame(mac_count_2), how='outer', on='vlan' )
mac_count_per_vlan.to_csv('_mac_count_per_vlan.csv',  sep=';', header=['mac_count_1', 'mac_count_2'])

mac_diff = df1.merge(df2, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']!='both']
mac_diff.to_csv('_mac_diff.csv', header=True, sep=';')

mac_stable = df1.merge(df2, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='both']
mac_stable.to_csv('_mac_stable.csv', header=True, sep=';')


mac_diff_per_vlan =  pd.DataFrame(mac_diff.groupby('vlan')['mac'].nunique())
mac_diff_per_vlan_vs_mac_count_per_vlan = pd.merge(mac_diff_per_vlan, mac_count_per_vlan, on='vlan')
mac_diff_per_vlan_vs_mac_count_per_vlan.to_csv('_mac_diff_per_vlan_vs_mac_count_per_vlan.csv', header=['mac_changed', 'mac_count_1', 'mac_count_2'], sep=';')


mac_diff_per_port =  pd.DataFrame(mac_diff.groupby('port')['mac'].nunique())
mac_diff_per_port_vs_mac_count_per_port = pd.merge(mac_diff_per_port, mac_count_per_port, on='port')
mac_diff_per_port_vs_mac_count_per_port.to_csv('_mac_diff_per_port_vs_mac_count_per_port.csv', header=['mac_changed', 'mac_count_1', 'mac_count_2'], sep=';')

###################


mac_grp1 = df1.groupby('mac')['vlan'].nunique()
non_unique_mac_1 = pd.DataFrame(mac_grp1[mac_grp1 != 1])
nu_mac_1 = df1.loc[df1['mac'].isin(non_unique_mac_1.index)].sort_values(by='mac')
nu_mac_1.to_csv('_nu_mac_1.csv',  sep=';', header=True)

df1_unique = pd.merge(df1, nu_mac_1, how = 'outer' , indicator=True).loc[lambda x : x['_merge']=='left_only']
df1_unique = df1_unique.drop(columns="_merge")
df1_unique.to_csv('_unique_mac_1.csv',  sep=';', header=True)

mac_grp2 = df2.groupby('mac')['vlan'].nunique()
non_unique_mac_2 = pd.DataFrame(mac_grp2[mac_grp2 != 1])
nu_mac_2 = df2.loc[df2['mac'].isin(non_unique_mac_2.index)].sort_values(by='mac')
nu_mac_2.to_csv('_nu_mac_2.csv',  sep=';', header=True)

df2_unique = pd.merge(df2, nu_mac_2, how = 'outer' , indicator=True).loc[lambda x : x['_merge']=='left_only']
df2_unique = df2_unique.drop(columns="_merge")
df2_unique.to_csv('_unique_mac_2.csv',  sep=';', header=True)


###################

mac_left = pd.merge(df1_unique, df2_unique, how = 'outer' , indicator=True).loc[lambda x : x['_merge']=='left_only']
mac_left.to_csv('_mac_left.csv', sep=';')

mac_right = pd.merge(df1_unique, df2_unique, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='right_only']
mac_right.to_csv('_mac_right.csv', sep=';')

mac_history_vlan = pd.merge(df1_unique, df2_unique, how='inner', on='mac')
mac_history_vlan = mac_history_vlan[(mac_history_vlan.vlan_x != mac_history_vlan.vlan_y)]
mac_history_vlan.to_csv('_mac_history_vlan.csv', sep=';')

mac_history_port = pd.merge(df1_unique, df2_unique, how='inner', on='mac')
mac_history_port = mac_history_port[(mac_history_port.port_x != mac_history_port.port_y)]
mac_history_port.to_csv('_mac_history_port.csv', sep=';')


######################


mac_count_1_unique = df1_unique.groupby('port')['mac'].count()
mac_count_2_unique = df2_unique.groupby('port')['mac'].count()
mac_count_per_port_unique = pd.DataFrame(mac_count_1_unique).merge(pd.DataFrame(mac_count_2_unique), how='outer', on='port' )
mac_count_per_port_unique.to_csv('_mac_count_per_port_unique.csv',  sep=';', header=['mac_count_1', 'mac_count_2'])


mac_count_1_unique = df1_unique.groupby('vlan')['mac'].count()
mac_count_2_unique = df2_unique.groupby('vlan')['mac'].count()
mac_count_per_vlan_unique = pd.DataFrame(mac_count_1_unique).merge(pd.DataFrame(mac_count_2_unique), how='outer', on='vlan' )
mac_count_per_vlan_unique.to_csv('_mac_count_per_vlan_unique.csv',  sep=';', header=['mac_count_1', 'mac_count_2'])

mac_diff_unique = df1_unique.merge(df2_unique, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']!='both']
mac_diff_unique.to_csv('_mac_diff_unique.csv', header=True, sep=';')

mac_stable_unique = df1_unique.merge(df2_unique, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='both']
mac_stable_unique.to_csv('_mac_stable_unique.csv', header=True, sep=';')


mac_diff_per_vlan_unique =  pd.DataFrame(mac_diff_unique.groupby('vlan')['mac'].nunique())
mac_diff_per_vlan_vs_mac_count_per_vlan_unique = pd.merge(mac_diff_per_vlan_unique, mac_count_per_vlan_unique, on='vlan')
mac_diff_per_vlan_vs_mac_count_per_vlan_unique.to_csv('_mac_diff_per_vlan_vs_mac_count_per_vlan_unique.csv', header=['mac_changed', 'mac_count_1', 'mac_count_2'], sep=';')


mac_diff_per_port_unique =  pd.DataFrame(mac_diff_unique.groupby('port')['mac'].nunique())
mac_diff_per_port_vs_mac_count_per_port_unique = pd.merge(mac_diff_per_port_unique, mac_count_per_port_unique, on='port')
mac_diff_per_port_vs_mac_count_per_port_unique.to_csv('_mac_diff_per_port_vs_mac_count_per_port_unique.csv', header=['mac_changed', 'mac_count_1', 'mac_count_2'], sep=';')



######################


df1 = pd.read_csv("_mac1.csv", sep=';')
df2 = pd.read_csv("_mac2.csv", sep=';')

if('age' in df1.columns and 'age' in df2.columns):
   
   mean_1 = df1.groupby('port')['age'].mean().round(0)
   mean_2 = df2.groupby('port')['age'].mean().round(0)
   age_per_port = pd.DataFrame(mean_1).merge(pd.DataFrame(mean_2), how='outer', on='port' )
   age_per_port.to_csv('_age_mode_per_port.csv',  sep=';', header=['age_1', 'age_2'])

   age_per_port_vs_mac_count = pd.merge(age_per_port, mac_count_per_port, on='port')
   age_per_port_vs_mac_count.to_csv('_age_mode_per_port_vs_mac_count.csv',  sep=';')



   mean_1 = df1.groupby('vlan')['age'].mean().round(0)
   mean_2 = df2.groupby('vlan')['age'].mean().round(0)
   age_per_vlan = pd.DataFrame(mean_1).merge(pd.DataFrame(mean_2), how='outer', on='vlan' )
   age_per_vlan.to_csv('_age_mode_per_vlan.csv',  sep=';', header=['age_1', 'age_2'])

   age_per_vlan_vs_mac_count = pd.merge(age_per_vlan, mac_count_per_vlan, on='vlan')
   age_per_vlan_vs_mac_count.to_csv('_age_mode_per_vlan_vs_mac_count.csv',  sep=';')

