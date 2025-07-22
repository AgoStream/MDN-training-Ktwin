import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from pathlib import Path
import glob


ROOT = Path(__file__).resolve().parents[2]  
plots_path = ROOT / "plots" / "data_plots"


T0 = pd.to_datetime('2024-01-26 00:00:00')
SYNC_T0 = False
EVAL_OVER = 1       # 0: Don't evaluate overlapping, 1: evaluate overlapping without proc. time fragmentation, 2: evaluate overlapping and proc. time fragmentation
MAX_REP = 1
span = 8 # it is used to differentiate the files in the traces folder
sd_list = [1] # slow down factor is currently not used, but it is here for future use

traces_path = ROOT / "data" / "dataset" 


for sd in sd_list:

    for rep in range(1, MAX_REP + 1):
        path = str(traces_path) + r"\traces_replica" + str(rep) + '\*.csv'
        filenames = glob.glob(path)

        for f in filenames:
            index_rps=f.find('rps') + 3 #save rps number

            # open each file and save it to dataframe, then gather a list of the operating services
            df = pd.read_csv(f, index_col=None)
            df.columns = ["trace_id", "span_id", "start_time", "duration", "client", "server", "grpc_path", "http_status_code",
                    "grpc_message", "http_method", "response_size", "request_size", "rep_id"]
            services_whole = df['server'].unique()

# this dicts of lists will be used later for storing infos about the services and dfs of the latter
service_lists = {service: [] for service in services_whole}
df_lists = {service: [] for service in services_whole} 

# this list is used to understand which service is calling which, will be used later to process the call chain                
service_calls = {}  

# for loop to process each service, opening back every file could be unnecessary
for service in services_whole:

    for sd in sd_list:

        for rep in range(1, MAX_REP + 1):

            path = str(traces_path) + r'\traces_replica' + str(rep) + '\*.csv'
            filenames = glob.glob(path) 

            for f in filenames:

                index_rps=f.find('rps') + 3 #save rps number
                df = pd.read_csv(f, index_col=None)  
                df.columns = ["trace_id", "span_id", "start_time", "duration", "client", "server", "grpc_path", "http_status_code",
                        "grpc_message", "http_method", "response_size", "request_size", "rep_id"]
                df['start_time'] = pd.to_datetime(df['start_time'], unit='us')
                df['duration']= df['duration'].apply(lambda x: x/1e3)  #switch from microseconds to milliseconds
                df['rep_id'] = df['rep_id'].apply(lambda x: x.split('~')[2])


                f = f.replace('.csv', '') # change filename

                
                df.insert(1,'rps', float(f[index_rps:index_rps+4]))    # add in the first column the nominal rps
                df.insert(2, 'rep', rep)
                if SYNC_T0:
                    df['start_time'] = df['start_time'] - (df['start_time'].min() - T0)

                df['client_id'] = df.rep_id.apply(lambda x: x.split('.')[0])

                all_services = set(df['server'].unique())  
                clients = set(df['client'].unique())  
                

                # general idea behind the code below: 
                #
                # 1- first if is used to check wether i am the entry point or not, if i am the entry point i am the only service that is only 
                # called by myself, thus i have no other client than myself
                #
                # 2- if i am not the entry point, every info relating me can be seen from two perspectives: mine and the client's (calling me)
                # thus i must use two sets of parameters indicated by suffss (my pov) and by suffcs (client's pov)
                #
                # 3- if i am the entry point i don't need two sets of parameters simply take into account my normal infos
                #
                # 4- to evaluate the real duration of services we must also check that there was no queue shaenanigans happening
                #
                # 5- as for the duration evaluation we also have to the into account the double perspective matter
                #
                # 6- we then store the df into a list collecting dfs for every service in the system
                #
                if not df[(df['server'] == service) & (df['client'] != service)].empty:
                    client = df.loc[(df['server'] == service) & (df['client'] != service), 'client'].unique()
                    client = client[0]

                    (server2, client2, server1, client1) = (service, service, service, client) # get both the service service and service client
                   
                    suffcs = f"_{client}_{service}" # define some suffixes for brevity 
                    suffss = f"_{service}_{service}"
                    
                    service_df = df[(df['server'] == server2) & (df['client'] == client2)].merge(df[(df['server'] == server1) & (df['client'] == client1)], # faccio il merge dei due campi che definiscono le chiamate sopra sotto lo stesso trace_id, prendo solo uguali trace_id
                                            on = ['trace_id'],
                                            suffixes = (suffss, suffcs))
                    service_df[str(service) + '_id'] = service_df['client_id' + suffss] # get the id of the current service
                    
                    
                    # get the unique rps and rep for the service
                    service_df['rps_' + str(service)] = service_df['rps' + suffcs] 
                    service_df['rep_' + str(service)] = service_df['rep' + suffcs]

                    # state the column names
                    service_df = service_df[['trace_id', 'rps_' + str(service) , 'rep_' + str(service),  str(service) + '_id','start_time' + suffcs,'duration'+ suffcs, 'start_time' + suffss, 'duration' + suffss]]
                    service_df = service_df.sort_values(by=['start_time' + suffss]) # order chronologically

                    
                    if EVAL_OVER == 1:
                        
                        # calculate end time
                        service_df['end_time' + suffss] = service_df['start_time' + suffss] + pd.to_timedelta(service_df['duration' + suffss], unit='ms')
                        # get the process time
                        service_df['proc_time_' + str(service)] = service_df.groupby([str(service) + '_id'])['end_time'+ suffss].diff().dt.total_seconds() * 1000
                        # check that there is no queue throught the booleans
                        service_df['proc_time_' + str(service)] = service_df['duration' + suffss] * (service_df['proc_time_' + str(service)] >= service_df['duration' + suffss]) + (service_df['proc_time_' + str(service)]) * (service_df['proc_time_' + str(service)] < service_df['duration' + suffss] )
                        # delay from the client to the server pov
                        service_df['delay_' + str(service)] = service_df['duration' + suffcs] - service_df['duration' + suffss]
                        # end time from the client pov
                        service_df['end_time' + suffcs] = service_df['start_time' + suffcs] + pd.to_timedelta(service_df['duration' + suffcs], unit='ms')
               

               # same logic here but for the entry point
                else:  
                    entry = service              
                    (server, client) = (service,service)
                    service_df = df[(df['server'] == server) & (df['client'] == client)]
                    
                    suffss = f"_{service}_{service}"
                    service_df['start_time' + suffss] = service_df.start_time
                    service_df['duration' + suffss] = service_df.duration
                    service_df[str(service)+ '_id'] = service_df.client_id
                    service_df['rps_' + str(service)] = service_df['rps'] 
                    service_df['rep_' + str(service)] = service_df['rep']
                    service_df = service_df[['trace_id', 'rps_' + str(service) , 'rep_' + str(service), 'start_time' + suffss,'duration' + suffss, str(service) + '_id']]
                    service_df = service_df.sort_values(by=['start_time' + suffss])
                    service_df['i_time'] = service_df['start_time' + suffss].diff().dt.total_seconds()
                    service_df['sd'] = sd

                    
                    if EVAL_OVER == 1:
                        
                        service_df['end_time' + suffss] = service_df['start_time' + suffss] + pd.to_timedelta(service_df['duration' + suffss], unit='ms')
                        
                        service_df['proc_time_' + str(service)] = service_df.groupby([str(service) + '_id'])['end_time'+ suffss].diff().dt.total_seconds() * 1000
                        
                        service_df['proc_time_' + str(service)] = service_df['duration' + suffss] * (service_df['proc_time_' + str(service)] >= service_df['duration' + suffss]) + (service_df['proc_time_' + str(service)]) * (service_df['proc_time_' + str(service)] < service_df['duration' + suffss] )
                        
                service_lists[service].append(service_df)  

                                         

# service_calls will become a list of the services that call other services, including infos on who they call

for service in df['server'].unique():
    if not df[(df['server'] != service) & (df['client'] == service)].empty:
        called_services = df[df['client'] == service]['server'].unique()  
        service_calls[service] = [s for s in called_services if s != service]

# generate dfs for every service
for service, value in service_lists.items():
    df_lists[service] = pd.concat(value, axis = 0, ignore_index = True)

# merge all the dfs in one big df
traces_new_df = list(df_lists.values())[0] 
for df in list(df_lists.values())[1:]: 
    traces_new_df = traces_new_df.merge(df, on=['trace_id'])

traces_new_df = traces_new_df.sort_values(by=[f'start_time_' + str(entry) + '_' + str(entry)]) # sorting by the time seen at the entrypoint


### SERVICE ARCHITECTURE LOGIC BELOW
# the following code deducts how the services behave, in a chronological sense, it keeps track of which is calling which in a dict
# the code operates trace by trace and checks every single call, deducing if two services are called in a parallel or cascade way
# it then calculates the processing time of each of the calling services taking into account how they operate


for service_calling in service_calls.keys():
    if service_calls[service_calling]:  # following logic needs to get infos on the how service calling other services works
        calls_info = []

        # group all the service calls
        for servizio_called in service_calls[service_calling]:
            start_col = f"start_time_{service_calling}_{servizio_called}"
            duration_col = f"duration_{service_calling}_{servizio_called}"

            if start_col in traces_new_df.columns and duration_col in traces_new_df.columns:
                end_time_col = traces_new_df[start_col] + pd.to_timedelta(traces_new_df[duration_col], unit="ms")
                calls_info.append(pd.DataFrame({
                    "trace_id": traces_new_df['trace_id'],  # creating a dataframe collecting all the traces of the called services and their timestamps
                    "called_service": servizio_called,
                    "start_time": traces_new_df[start_col],
                    "end_time": end_time_col,
                    "duration": traces_new_df[duration_col]
                }))

        # if any calls were found
        if calls_info:

            # chronological sort
            calls_df = pd.concat(calls_info).sort_values(by=["start_time"]).reset_index(drop=True)  

            
            # initialize variables
            cascade = []
            parallel = []
            parallel_group = []
            end_group_max = None
            current_trace = None

            for idx, row in calls_df.iterrows():
                trace_id = row['trace_id']
                start = row['start_time']
                end = row['end_time']
                duration = row['duration']

                # if it's changing from one trace to the next check how the last trace ended and close parallel group if it was opened, 
                # if that's not the case put the last call into cascade list since it must be a cascade
                if current_trace != trace_id:
                    if parallel_group:
                
                        # if parallel group has more than 1 item it means there is still unclosed parallel 
                        if len(parallel_group) > 1:
                            parallel.append((current_trace, parallel_group))
                        else:
                            cascade.append((current_trace, parallel_group[0])) 
                    # reset values for new trace
                    parallel_group = []
                    end_group_max = None
                    current_trace = trace_id

                # everytime put the first call into parallel group then decide if its parallel or cascade
                if not parallel_group:
                    parallel_group.append((start, end, duration))
                    end_group_max = end
                else:
                    # check if the call is parallel
                    if start <= end_group_max:
                        # if its parallel append it into parallel group and get the current max of the end time
                        parallel_group.append((start, end, duration))
                        end_group_max = max(end_group_max, end)
                    else:
                        # if its cascade first close the current parallel group and put it into the parallel dict
                        if len(parallel_group) > 1:
                            parallel.append((current_trace, parallel_group))

                        # then put the current timestamps into series dict
                        cascade.append((current_trace, parallel_group[0]))  

                        # reset
                        parallel_group = [(start, end, duration)]
                        end_group_max = end
            

            # logic to handle the last trace of the df
            if parallel_group:
                if len(parallel_group) > 1:
                    parallel.append((current_trace, parallel_group))
                else:
                    cascade.append((current_trace, parallel_group[0]))

            # calculate the values: first subtract to get the real process time into below dict
            subtract_per_trace = {}

            # cascade: simple logic get the sum of all the durations
            for trace_id, (start, end, duration) in cascade:
                subtract_per_trace.setdefault(trace_id, 0)
                subtract_per_trace[trace_id] += duration

            # parallel: get the first service that is called in the parallel case and get the last time to finish, then calculate the whole parallel duration
            for trace_id, group in parallel:
                start_min = min([x[0] for x in group])
                end_max = max([x[1] for x in group])
                duration_eff = (end_max - start_min).total_seconds() * 1000  # ms
                subtract_per_trace.setdefault(trace_id, 0)
                subtract_per_trace[trace_id] += duration_eff

            # now subtract
            for trace_id, minuend in subtract_per_trace.items():
                mask = traces_new_df['trace_id'] == trace_id
                traces_new_df.loc[mask, f'proc_time_{service_calling}'] -= minuend
            



# clean up proces and add rps_eff columns

traces_new_df['group'] = traces_new_df.index // 8
for service in services_whole:
    mean_values = traces_new_df.groupby(['group', 'rps_'+ str(service), 'rep_' + str(service)])['i_time'].transform('mean')
    traces_new_df['rps_eff_' + str(service)] = 1/mean_values
    traces_new_df['rps_eff_' + str(service)] = traces_new_df['rps_eff_' + str(service)].fillna(traces_new_df['rps_' + str(service)])

# cleanup
traces_new_df = traces_new_df.dropna()

# save csv
traces_new_df.to_csv(str(traces_path) + r"\newSL" + str(span) + ".csv", index= False)

