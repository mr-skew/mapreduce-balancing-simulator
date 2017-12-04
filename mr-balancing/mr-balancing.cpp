/*
 * MapReduce-balancing-simulator
 * Copyright (C) 2017 MapReduce-balancing-simulator creators
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "stdafx.h"

using namespace std;


#pragma region constants

const int MAX_PROC=10000;       //upper limit on m, r
const int MAX_KEY_NUM=1000000;  //upper limit on key_num
const int PARAMNUMBER=13;       //number of input parameters
const double MAX_REAL=1e100;
const double eps=1e-10;

#pragma endregion

#pragma region input data

double V;                     //load size
int m,r;                      // number of mappers, reducers
double C;                     // communication rate
int bisection;                //bisection width limit
double A;                     //mapper computation rate
double a_sort;                //sorting rate
double a_red;                 //reducing rate
double a_master;              //master computation rate
double comm_master_size;      //size of message sent to/from master
double gamma;                 //result multiplicity fraction
double gamma1;			      //result multiplicity fraction for reducers
int key_num;                  //number of keys
double keyloads[MAX_KEY_NUM]; //fraction of load with given key; the same for all mappers

#pragma endregion

#pragma region red_part structure
/*
red_part is a structure used for choosing the least loaded reducers from priority queue
in master computations for static and divide_keys method
*/
struct red_part
{
  int red_num;
  double red_load;
  double sort_time;
  red_part(int num, double load, double time)
  {
    red_num=num;
    red_load=load; 
    sort_time=time;
  }
};
class red_part_compare
{
public:
  bool operator() (const red_part &lhs, const red_part &rhs) const
  {
    return lhs.red_load>rhs.red_load;
  }
};
#pragma endregion

#pragma region comm_merge_event structure
/*
comm_merge_event is a structure holding events in merge communication stage of divide_keys method
*/
struct comm_merge_event
{
  int from, to;       //which reducers communicate
  double load_left;   //how much left to send;
  double start_from;  //the earliest possible start
  bool started;       //true if already started

  comm_merge_event(int _from, int _to, double load, double from_time)
  {
	from=_from;
	to=_to;
	load_left=load;
	start_from=from_time;
	started=false;
  } 
};
#pragma endregion

#pragma region data structures for divide_keys method
/*
part_function is the partitioning function constructed in divide_keys_method;
for each key we have a vector because a key may be split between several reducers;
the elements of vectors are pairs: reducer number (int), frequency (double)
*/
vector<pair<int, double> > part_function[MAX_KEY_NUM]; 

double load_to_merge[MAX_PROC]={0};  //amounts of load to be merged by reducers in divide_keys method
int pieces_to_merge[MAX_PROC]={0};   //how many pieces of data a reducer merges together in divide_keys method

#pragma endregion

#pragma region utility functions
/*
comm_rate() computes communicaton rate for given number of open connections     
*/
double comm_rate(int comm_num)
{
  return max(C,C*comm_num/bisection); 
}

/*
sort_time() returns time of sorting given amount of data 
*/
double sort_time(double x)
{
  if (x<=eps) return 0; 
  if (x<2) return a_sort*x; //do not multiply by log_2(x) if it is smaller than 1
  return a_sort*(x)*log(x)/log(2.0);
}

/*
get_reduced_load() computes what amount of final results will be produced by a reducer from received load 
*/
double get_reduced_load(double load)
{
  //gamma1=0 means we get only a single value for a key
  if (gamma1<eps)
	return comm_master_size;
  //otherwise, load size is multplied by gamma1
  return gamma1*load;
}

/*
merge_time() computes merging time for load of size x in the final stage of divide_keys method on reducer red
*/
double merge_time(double x, int red)
{
  return a_red*x*log(pieces_to_merge[red])/log(2.0);
}

/*
dispersion_measures() computes stdev and IQR for reducer finish times
*/
void dispersion_measures(double *times, int r, double &IQR, double &stdev)
{
  sort(times,times+r);
  double q1,q3;
  //1st quartile
  if ((r+1)%4==0)
    q1=times[(r+1)/4-1];
  else
    q1=(times[(r+1)/4-1]+times[(r+1)/4])/2;
  //3rd quartile
  if ((r+1)%4==0)
    q3=times[3*(r+1)/4-1];
  else
    q3=(times[3*(r+1)/4-1]+times[3*(r+1)/4])/2;
  IQR=q3-q1;
  //stddev
  double mean=0.0;
  for (int i=0;i<r;i++)
    mean+=times[i];  
  mean/=r;
  stdev=0.0;
  for (int i=0;i<r;i++)
    stdev+=(times[i]-mean)*(times[i]-mean);
  stdev/=r;
  stdev=sqrt(stdev);  
}

#pragma endregion

#pragma region input reading functions

/*
keyvalue() reads key value from 1 line  
srcstring - source string with key value pair
key - value of the key string
value - read value;
if key is found in the string, it returns value, otherwise (key not found) returns 0
key string must start at the beginning of the line
*/
int keyvalue(char *srcstring, char *key, double *value)
{
  char *p2eq=NULL;
  value;
  p2eq=strstr(srcstring,key);
  if ((p2eq!=NULL) && (p2eq==srcstring)) // key is found starting at the beginnig of line
  {
    sscanf_s(p2eq,"%*s %lf",value); 
    return 1;
  }
  else return 0;
}

/*
dataread() reads input data from filename 

Format of input file:

";" at the beginning of the line means it is a comment and this line is ignored;
comments are allowed at the beginning of the file only

* first the system/application parameters are read in the form: key value \n
; keys are {V, C, A, a_sort, a_red, a_master, comm_master_size, gamma, m, r, bisection, key_num} 
; the key value pair must start at the beginning of the line
; everything that follows after the key value pair in the current line is ignored

* then m sequences of key_num frequencies of the keys; no comments allowed now
; each sequence is for one mapper
; each sequence starts at the beginning of the line
; it is assumed that the contents of the file are correct and concise
*/
void dataread(_TCHAR* filename)
{
  int paramcnt=PARAMNUMBER;      // number of parameters to read
  ifstream infile;               // input file
  char ohneline[100000];	 	 // to read one line of input file
  bool ParamIsRead[PARAMNUMBER]; // flags of read parameters
  int i;                         // auxiliary
  double aux_val;                // auxiliary double

  wcout << "reading file: " << filename << '\n';
  infile.open(filename,ios::in);
  if (!infile)
  {
    cout << "file " << filename << " does not exist";
    exit(EXIT_FAILURE);
  }

  //set flags of parameters to unread  
  for(i=0;i<PARAMNUMBER;i++) 
    ParamIsRead[i]=false;  

  //read 12 key parameters
  while (paramcnt>0)
  {
    infile.getline(ohneline,1000,'\n');
    if (ohneline[0]==';') continue;
    if ((!ParamIsRead[0]) && (keyvalue(ohneline, "V",&V))) {paramcnt--; ParamIsRead[0]=true; continue; } 
    if ((!ParamIsRead[1]) && (keyvalue(ohneline, "C",&C))) {paramcnt--; ParamIsRead[1]=true; continue; } 
    if ((!ParamIsRead[2]) && (keyvalue(ohneline, "A",&A))) {paramcnt--; ParamIsRead[2]=true; continue;} 
    if ((!ParamIsRead[3]) && (keyvalue(ohneline, "a_sort",&a_sort))) {paramcnt--; ParamIsRead[3]=true; continue;} 
    if ((!ParamIsRead[4]) && (keyvalue(ohneline, "a_red",&a_red))) {paramcnt--; ParamIsRead[4]=true; continue;} 
    if ((!ParamIsRead[5]) && (keyvalue(ohneline, "a_master",&a_master))) {paramcnt--; ParamIsRead[5]=true; continue;} 
    if ((!ParamIsRead[6]) && (keyvalue(ohneline, "comm_master_size",&comm_master_size))) {paramcnt--; ParamIsRead[6]=true; continue;} 
    if ((!ParamIsRead[7]) && (keyvalue(ohneline, "gamma",&gamma))) {paramcnt--; ParamIsRead[7]=true; continue;} 
	if ((!ParamIsRead[12]) && (keyvalue(ohneline, "gamma1",&gamma1))) {paramcnt--; ParamIsRead[12]=true; continue;} 
    if (!ParamIsRead[8])
    {
      i=keyvalue(ohneline, "m",&aux_val);
      if (i==1) { m=int(aux_val); paramcnt--; ParamIsRead[8]=true; continue;}
    }
    if (!ParamIsRead[9])
    {
      i=keyvalue(ohneline, "r",&aux_val);
      if (i==1) { r=int(aux_val); paramcnt--; ParamIsRead[9]=true; continue;}
    }
    if (!ParamIsRead[10])
    {
      i=keyvalue(ohneline, "bisection",&aux_val);
      if (i==1) { bisection=int(aux_val); paramcnt--; ParamIsRead[10]=true; continue;}
    }
    if (!ParamIsRead[11])
    {
      i=keyvalue(ohneline, "key_num",&aux_val);
      if (i==1) { key_num=int(aux_val); paramcnt--; ParamIsRead[11]=true; continue;}
    }
  }
  //read key frequencies  
  for (i=0;i<key_num;i++)    
    infile>>keyloads[i];          

  //confirm parameters read (without key frequencies)
  cout << "V= " << V <<"\n";
  cout << "C= " << C << " " <<"\n";
  cout << "A= " << A << " " <<"\n";
  cout << "a_sort= " << a_sort <<"\n";
  cout << "a_red= " << a_red <<"\n";
  cout << "a_master= " << a_master <<"\n";
  cout << "comm_master_size= " << comm_master_size <<"\n";
  cout << "gamma= " << gamma <<"\n";
  cout << "gamma1= " << gamma1 <<"\n";
  cout << "m= " << m << "\n";
  cout << "r= " << r << "\n"; 
  cout << "bisection= " << bisection <<"\n";
  cout << "key_num= " << key_num <<"\n";
    
  infile.close();
}

#pragma endregion

#pragma region reference algorithm
/*
send_data() computes time needed to send data between mappers and reducers, for amounts of load given in keyloads[]
and computes the starting time of computations for each reducer in red_comm_start[];
sending starts at current_time;
the last argument overrides the number of reducers for cases when not all reducers have to communicate
*/
double send_data(double current_time,double *load_to_send, double *red_comm_start, int r)
{
  int comm_num; 
  double comm_time=current_time;
  int comm_with[MAX_PROC]={0}; //-1 means idle (no communication)  
  double load_left[MAX_PROC]={0};
  if (m<r)
  {
    bool red_busy[MAX_PROC]={false};
    //mapper i sends to reducers i, i+1, ..., 1, ..., i-1
	//comm_with[] and load_left[] are defined for mappers
    for (int i=0;i<m;i++)
    {
      comm_with[i]=i;
      load_left[i]=load_to_send[i]; 
      red_busy[i]=true;
    }
    comm_num=m;
    
    while (1)
    {                
      //find next event - for mappers which did not finish communications and sent some load since the previous event
      double next_time=MAX_REAL;     
      for (int i=0;i<m;i++)
        if (comm_with[i]!=-1 && load_left[i]>0 && load_left[i]*comm_rate(comm_num)<next_time) 
          next_time=load_left[i]*comm_rate(comm_num);
      if (next_time==MAX_REAL) break; //no further events, comm. finished  
      
      comm_time+=next_time;
      int comm_finished_num=0; 
      //finish communications
      for (int i=0;i<m;i++) if (load_left[i]>=eps) //if some communication was executed
      {
        load_left[i]-=next_time/comm_rate(comm_num);      
        if (load_left[i]<eps) //commmunication finished
        {
          red_busy[comm_with[i]]=false;
          //update red_comm_start, it will hold the largest time when communication of reducer with some mapper ends
          red_comm_start[comm_with[i]]=comm_time;
          load_left[i]=0;
          comm_finished_num++;       
        } 
      }
      //now comm_num can be updated
      comm_num-=comm_finished_num;
      
      //start new communications for mappers which did not finish all communications, but are free at the moment
      for (int i=0;i<m;i++) if (comm_with[i]!=-1 && load_left[i]<eps)
      {
        int next_proc=(comm_with[i]+1)%r;
        if (next_proc==i) //cycle finished
          comm_with[i]=-1;
        else
        {
          if (!red_busy[next_proc]) //if possible, start new communication
          {
            comm_with[i]=next_proc;
            red_busy[next_proc]=true;
            load_left[i]=load_to_send[next_proc];
            comm_num++;       
          }         
        }
      }
    }
  }
  else //if (m>=r)
  {
    bool map_busy[MAX_PROC]={false};
    //reducer i receives from mappers i, i+1, ..., 1, ..., i-1
	//comm_with[] and load_left[] are defined for reducers
    for (int i=0;i<r;i++)
    {
      comm_with[i]=i;
      load_left[i]=load_to_send[i]; 
      map_busy[i]=true;
    }
    comm_num=r;
    while (1)
    {     
      //find next event - for mappers which did not finish communications and sent some load since the previous event
      double next_time=MAX_REAL;     
      for (int i=0;i<m;i++)
        if (comm_with[i]!=-1 && load_left[i]>0 && load_left[i]*comm_rate(comm_num)<next_time) 
          next_time=load_left[i]*comm_rate(comm_num);
      if (next_time==MAX_REAL) break; //no further events, comm. finished

      comm_time+=next_time;
      int comm_finished_num=0; 
      //finish communications
      for (int i=0;i<r;i++) if (load_left[i]>=eps) //if some communication was executed
      {
        load_left[i]-=next_time/comm_rate(comm_num);     
        if (load_left[i]<eps) //commmunication finished
        {
          map_busy[comm_with[i]]=false;
          //update red_comm_start, it will hold the largest time when communication of reducer with some mapper ends
          red_comm_start[i]=comm_time;
          load_left[i]=0;
          comm_finished_num++;     
        } 
      }
      //now comm_num can be updated
      comm_num-=comm_finished_num;
      
      //start new communications for reducers which did not finish all communications, but are free at the moment
      for (int i=0;i<r;i++) if (comm_with[i]!=-1 && load_left[i]<eps)
      {
        int next_proc=(comm_with[i]+1)%m;
        if (next_proc==i) //cycle finished
          comm_with[i]=-1;
        else
        {
          if (!map_busy[next_proc]) //if possible, start new communication
          {
            comm_with[i]=next_proc;
            map_busy[next_proc]=true;
            load_left[i]=load_to_send[i];
            comm_num++;         
          }          
        }
      }
    }
  }
  return comm_time;
}

/*
do_nothing_method() computes running time for no load balancing (reference algorithm);
key i goes to partition i%*r
*/
double do_nothing_method(double &IQR, double &stdev)
{
  double total_time=A*V/m;
  //mapping finished   
  //compute load amounts to transfer from one mapper to each reducer
  double load_to_send[MAX_PROC]={0};
  for (int i=0;i<key_num;i++)
    load_to_send[i%r]+=(gamma*V/m)*keyloads[i];
  
  //overhead for communicating with master (both mappers and reducers)
  total_time+=2*C*m*r*comm_master_size;
  
  //send load to reducers    
  double red_comm_start[MAX_PROC];
  double comm_time=send_data(total_time,load_to_send,red_comm_start,r);   
  
  //find the reducer finishing last
  double max_red_time=0,min_red_time=MAX_REAL;
  double red_finish[MAX_PROC];
  for (int i=0;i<r;i++)
  {
    red_finish[i]=red_comm_start[i] + sort_time(load_to_send[i]*m) + a_red*(load_to_send[i]*m);
    if (red_finish[i]>max_red_time)
      max_red_time=red_finish[i];
      
    if (red_finish[i]<min_red_time)
      min_red_time=red_finish[i];       
  }
  //total time is the time when the last reducer finishes
  total_time=max_red_time;

  //compute dispersion of relative reducer finish times
  for (int i=0;i<r;i++)
    red_finish[i]/=max_red_time;
  dispersion_measures(red_finish,r,IQR,stdev);
  
  return total_time;
}
#pragma endregion

#pragma region static algorithm
/*
static_load_partitionig() computes assignment of partitions to reducers as the master does in the static method,
and stores the results in load_to_send[];
it also computes sorting times for all reducers; each partition on a reducer is sorted separately!
the last argument is used in the mixed method to override the global key_num
in the static algorithm this is just the total key number
*/
void static_load_partitionig(int k, double *load_to_send, double *sort_times, int key_num)
{
  //part_sizes - partition sizes of A SINGLE mapper
  double *part_sizes=new double [k*r];
  for (int i=0;i<k*r;i++)
    part_sizes[i]=0;
  for (int i=0;i<key_num;i++)
    part_sizes[i%(k*r)]+=(gamma*V/m)*keyloads[i];  
  sort (part_sizes,part_sizes+k*r);
  priority_queue<red_part, vector<red_part>, red_part_compare> q;
  for (int i=0;i<r;i++)
    q.push(red_part(i,0,0));
  for (int i=k*r-1;i>=0;i--)
  {
    red_part chosen_red=q.top();
    q.pop();
    chosen_red.red_load+=part_sizes[i];
    //new partition added - update sorting time; load amount is part_sizes[i]*m  (from all mappers)
    chosen_red.sort_time+=sort_time(part_sizes[i]*m);
    q.push(chosen_red);
  }
  while (!q.empty())
  {
    red_part chosen_red=q.top();
    q.pop();
    load_to_send[chosen_red.red_num]=chosen_red.red_load;
    sort_times[chosen_red.red_num]=chosen_red.sort_time;
  }       
  delete [] part_sizes;
}

/*
static_method() computes running time for load balancing with the static method;
each mapper creates k*r partitions; key i goes to partition i%(k*r)
*/
double static_method(int k,double &IQR, double &stdev, double &balancecost)
{
  double total_time=A*V/m;
  //mapping finished 
  
  //compute load amounts to transfer from one mapper to each reducer, using static_load_partitionig
  double load_to_send[MAX_PROC]={0};
  double red_sort_time[MAX_PROC]={0};
  static_load_partitionig(k, load_to_send,red_sort_time,key_num);
  
  //overhead for master computations
  total_time+=a_master*(k*r*m + k*r*log(k*r)/log(2) + k*r*log(r)/log(2));
  
  //overhead for communicating with master (both mappers and reducers)
  total_time+=2*C*k*m*r*comm_master_size;

  //costs of load balancing are master communication and computation
  balancecost=a_master*(k*r*m + k*r*log(k*r)/log(2) + k*r*log(r)/log(2)) + 2*C*k*m*r*comm_master_size;
  
  //send load to reducers    
  double red_comm_start[MAX_PROC];
  double comm_time=send_data(total_time,load_to_send,red_comm_start,r);
  
  //find the reducer finishing last
  double max_red_time=0,min_red_time=MAX_REAL;
  double red_finish[MAX_PROC];
  for (int i=0;i<r;i++)
  {
    //use reducer sorting times computed when assigning partitions
    red_finish[i]=red_comm_start[i]+red_sort_time[i] + a_red*(load_to_send[i]*m);
    if (red_finish[i]>max_red_time)
      max_red_time=red_finish[i];
      
    if (red_finish[i]<min_red_time)
      min_red_time=red_finish[i];     

  }
  //total time is the time when last reducer finishes
  total_time=max_red_time;

  //compute dispersion of relative reducer finish times
  for (int i=0;i<r;i++)
    red_finish[i]/=max_red_time;
  dispersion_measures(red_finish,r,IQR,stdev);

  return total_time;    
}

#pragma endregion

#pragma region mixed algorithm
/*
mixed_method() executes mixed algorithm;
first_frac is the fraction of data that is divided between the reducers in the first stage,
parts1 is the number of partitions PER REDUCER in this first stage,
parts2 is the number of partitions PER REDUCER in the second stage. 
We operate on key numbers, not load amounts; the number of keys per partition in each step has to be integer.

Data from the first stage are sent all at once, like in the static method.
Data from the second stage are only sent after request from an idle reducer.
*/
double mixed_method(double first_frac, int parts1, int parts2,double &IQR, double &stdev, double &balancecost)
{ 
  int first_key_num=(int)(first_frac*key_num);
  int second_key_num=key_num - first_key_num;
  //check correctness of first_frac, parts1, parts2 - do partitions contain integer numbers of keys?
  if (first_key_num % (parts1*r) !=0 || second_key_num % (parts2*r) !=0)
  {
    cout<<"ERROR! Non-integer amount of keys in partition"<<endl;
    cout<<"first_key_num "<<first_key_num<<"parts1*r "<<parts1*r<<"second_key_num "<<second_key_num<<"parts2*r "<<parts2*r<<endl;
    return 0;
  }
  if (second_key_num==0) //in this case there is no second stage; change parts2 to handle this
    parts2=0;

  //first stage: 
  
  double total_time=A*V/m;
  //mapping finished 
  
  //compute load amounts to transfer from one mapper to each reducer, using static_load_partitionig
  //this means we gain some time by sorting smaller partitions on reducers instead of the whole load together
  double load_to_send[MAX_PROC]={0};
  double red_sort_time[MAX_PROC]={0};
  static_load_partitionig(parts1, load_to_send,red_sort_time,first_key_num);
  
  //overhead for master computations
  total_time+=a_master*(parts1*r*m + parts1*r*log(parts1*r)/log(2) + parts1*r*log(r)/log(2));
  
  //overhead for communicating with master (both mappers and reducers)
  total_time+=2*C*parts1*m*r*comm_master_size;

  //costs of load balancing are master communication and computation
  balancecost=a_master*(parts1*r*m + parts1*r*log(parts1*r)/log(2) + parts1*r*log(r)/log(2)) + 2*C*parts1*m*r*comm_master_size;
  
  //send load to reducers    
  double red_comm_start[MAX_PROC];
  double comm_time=send_data(total_time,load_to_send,red_comm_start,r);
  
  ///compute the times when reducers become idle and find the first idle reducer
  double red_idle[MAX_PROC];
  int first_idle=0;
  for (int i=0;i<r;i++)
  {
    //use reducer sorting times computed when assigning partitions
    red_idle[i]=red_comm_start[i]+red_sort_time[i] + a_red*(load_to_send[i]*m);
    if (red_idle[i]<red_idle[first_idle])
      first_idle=i;
  }
  
  //second stage:

  //compute what is to send - partition sizes of A SINGLE mapper
  double *part_sizes=new double [parts2*r];
  for (int i=0;i<parts2*r;i++)
    part_sizes[i]=0;
  //keys with IDs smaller than first_key_num were in the first stage; now take the rest   
  for (int i=first_key_num;i<key_num;i++)
    part_sizes[i%(parts2*r)]+=(gamma*V/m)*keyloads[i];
    
  //cur_part - number of the current partition to be sent; total number of partitions is  parts2*r
  int cur_part=0;
  while (cur_part<parts2*r) 
  {
    //check which reducers want more load, i.e. which are idle at this time (control_time)
    double control_time=red_idle[first_idle];
    for (int i=cur_part;i<min(cur_part+r,parts2*r);i++)
      control_time+=C*m*part_sizes[i];
      
    //store these reducers as pairs: (idle time, red number), so that sorting is easy
    vector<pair<double, int> > active_reds;
    for (int i=0;i<r;i++)
      if (red_idle[i]<control_time)
        active_reds.push_back(make_pair(red_idle[i],i));
    //sort according to increasing times: if less then r parts available, they go to the least loaded reducers        
    sort(active_reds.begin(),active_reds.end());
    
    //compute communication schedule with send_data():
      
    //starting time is first idle reducer time plus communication between reducers and master
    //reducers say if they need more load, master informs about partitions to take
    total_time=red_idle[first_idle]+C*(2*r-1)*comm_master_size;
    int cur_r=active_reds.size(); //the number of reducers communicating
    if (cur_part+cur_r>parts2*r) //but it cannot be greater than the amount of partitions
      cur_r=parts2*r-cur_part;
   
    //fill load_to_send[] with sizes of cur_r partitions
    for (int i=0;i<cur_r;i++)
      load_to_send[i]=part_sizes[cur_part+i];
    //red_comm_start can be reused now
    send_data(total_time,load_to_send,red_comm_start,cur_r);
    
    //update reducer idle times
    for (int i=0;i<cur_r;i++)
    {
      //processing next partition starts at max(idle,comm_start)
      if (red_comm_start[i]>red_idle[active_reds[i].second]) 
        red_idle[active_reds[i].second]=red_comm_start[i];
      //add sorting and processing time
      red_idle[active_reds[i].second]+= sort_time(load_to_send[i]*m) + a_red*(load_to_send[i]*m);
    }
    
    //update the number of current partition to send
    cur_part+=cur_r;
    //find the first idle reducer
    first_idle=0;
    for (int i=0;i<r;i++)
    {
      if (red_idle[i]<red_idle[first_idle])
        first_idle=i;
    }  
  }
  //the last reducer idle time is the schedule length
  for (int i=0;i<r;i++)
  {
    if (red_idle[i]>total_time)
      total_time=red_idle[i];
  }

  //compute dispersion of relative reducer finish times
  for (int i=0;i<r;i++)
    red_idle[i]/=total_time;
  dispersion_measures(red_idle,r,IQR,stdev);

  delete [] part_sizes;
  return total_time;
}
#pragma endregion

#pragma region multi-dynamic algorithm
/*
move_to_time() updates the loads remaining to be processed by reducers when moving from time curr_time to time what_time
*/
void move_to_time(double *load_remaining, double &curr_time, double what_time)
{
  if (what_time<curr_time)
    cout<<"ERROR! Moving to the past, not the future"<<endl;
  for (int i=0;i<r;i++)
  {
    load_remaining[i]-=(what_time-curr_time)/a_red;
    if (load_remaining[i]<0)
      load_remaining[i]=0;
  }
  curr_time=what_time;
}

/*
new_dynamic_method() computes running time for load balancing with the multi-dynamic algorithm;
key i goes to partition i%*r;
additional output parameter best_transfer_num: number of pairs of reducers transferring load
*/
double new_dynamic_method(int &best_transfer_num, double &IQR, double &stdev, double &balance_total_time, double &balance_sum_of_times)
{
   double total_time=A*V/m;
  //mapping finished  

  //compute load amounts to transfer from one mapper to each reducer and store amounts of load connected to keys;
  //they will be necessary to compute exact amounts of load transferred between reducers
  vector<double> red_keyloads[MAX_PROC];
  double load_to_send[MAX_PROC]={0};
  for (int i=0;i<key_num;i++)
  {
    load_to_send[i%r]+=(gamma*V/m)*keyloads[i];
    red_keyloads[i%r].push_back((gamma*V)*keyloads[i]); //total load on reducer -> without dividing by m
  }
  
  //overhead for communicating with master (both mappers and reducers)
  total_time+=2*C*m*r*comm_master_size;
  
  //send load to reducers    
  double red_comm_start[MAX_PROC];
  double comm_time=send_data(total_time,load_to_send,red_comm_start,r);  
  //find the reducer that finishes first and the times when all reducers finish sorting
  double min_red_time=MAX_REAL;
  double sort_stop[MAX_PROC],last_sort_stop=0;
  int who_stops_first=-1;

  double red_finish[MAX_PROC];
  
  for (int i=0;i<r;i++)
  {
    sort_stop[i]=red_comm_start[i]+ sort_time(load_to_send[i]*m); 
    if (sort_stop[i]>last_sort_stop)    
      last_sort_stop=sort_stop[i];    
    double curr_time=sort_stop[i] + a_red*(load_to_send[i]*m);
    red_finish[i]=curr_time; //initial value - finish time with no balancing
    if (curr_time<min_red_time)
    {
      min_red_time=curr_time;
      who_stops_first=i;
    }    
  } 
  
  //if necessary, wait until all reducers finish sorting
  if (last_sort_stop>min_red_time)
    min_red_time=last_sort_stop;

  //the initial part ends here, and balancing reducer loads starts  

  balance_total_time=0;
  balance_sum_of_times=0;
  double balance_intvls[1000][2];
  int ibal=0;

  double curr_time=min_red_time;
  //available_from - the time when an assigned part of load from another reducer is received
  double available_from[MAX_PROC]={0};
  
  //time for gathering data about reducers, choosing the one to hand over load and inform it:    
  double time_master_decision=C*(r+1)*comm_master_size+ a_master*r;
  //compute load remaining to process on each reducer at the moment curr_time+time_master_decision
  double load_remaining[MAX_PROC];
  for (int i=0;i<r;i++) 
  {
    double processing_time=(curr_time + time_master_decision)-sort_stop[i];
    double load_processed=processing_time/a_red;    
    load_remaining[i]= (load_to_send[i]*m) - load_processed;
    if (load_remaining[i]<0) load_remaining[i]=0;
  }
  //main processing loop
  while (1)
  {
    //search for the reducers to share load
    int max_load_red=0;   //number of reducer to share load with another
    int min_load_red=0;   //number of idle reducer (any of them, if there are more than one)
    int min_av=0;         //find also min available time (but greater than curr_time) 
    int how_many_av=0;    //and how many reducers are available
    for (int i=0;i<r;i++) 
    {      
      if ((load_remaining[i]>load_remaining[max_load_red] && available_from[i]<=curr_time) || available_from[max_load_red]>curr_time)
        max_load_red=i;
      if ((load_remaining[i]<load_remaining[min_load_red] && available_from[i]<=curr_time) || available_from[min_load_red]>curr_time)
        min_load_red=i;
      if ((available_from[i]<available_from[min_av] && available_from[i]>curr_time) || (available_from[min_av]<=curr_time))
        min_av=i;
      if (available_from[i]<=curr_time)
        how_many_av++;
    }
    if (load_remaining[min_load_red]>eps)//no idle reducers - wait until this one is idle
    {
      double wait_time=load_remaining[min_load_red]*a_red;
      move_to_time(load_remaining, curr_time, curr_time+wait_time);
      load_remaining[min_load_red]=0;//to avoid rounding errors
      continue;
    }
    
    if (how_many_av<2) //we have to wait until more reducers are available
    {
      //move to min available time
      move_to_time(load_remaining, curr_time,available_from[min_av]);      
      continue;
    }
    //finish the algorithm if everyone finished processing
    if (load_remaining[max_load_red]<eps)
      break;
    
    //compute amount of load to send
    int transfer_num=1; 
    double comm_r=comm_rate(transfer_num);
    double load=(a_red/(comm_r+2*a_red))*load_remaining[max_load_red];//load size computed by the master
    
    //compute the real load to be sent - whole keys, starting from the last one, not more than "load"
    //also, check which keys are already processed and hence cannot be transferred
    double processing_time=(curr_time + time_master_decision)-sort_stop[max_load_red];
    double load_processed=processing_time/a_red;//we don't have to take into account idle times; they are handled by adding fake loads on reducers in the later code
    int keys_processed=0;//number of keys whose processing already started on max_load_red      
    for (int i=0;i<red_keyloads[max_load_red].size();i++)
    {
      load_processed-=red_keyloads[max_load_red][i];
      keys_processed++;
      if (load_processed<0) break;
    }
    
    //check which keys to transfer
    double real_load=0;
    int how_many_keys=0;
    for (int i=red_keyloads[max_load_red].size()-1;i>=keys_processed;i--)
    {
      if (real_load + red_keyloads[max_load_red][i]<=load+eps)
      {
        real_load+=red_keyloads[max_load_red][i];
        how_many_keys++;
      }
      else break;
    }
    //and change load to the real  value
    load=real_load;
    
    //it no load to send, then make more reducers available or finish
    if (how_many_keys==0)
    {   
      if (how_many_av<r)
      {
        move_to_time(load_remaining, curr_time,available_from[min_av]);
        continue;
      }
      else //nothing to do
        break;
    }
         
    //transfering load between reducers:

    best_transfer_num++;
    //processor can send load (and obtain new) only after it is received / sent
    available_from[min_load_red]=curr_time + time_master_decision+comm_r*load; 
    available_from[max_load_red]=curr_time + time_master_decision+comm_r*load; 

    //balance costs as sum of communication lengths and master decision times
    balance_sum_of_times+=time_master_decision+comm_r*load; 
    //but we also want all balancing communication intervals for the second measure (total length of intervals)
    balance_intvls[ibal][0]=curr_time;
    balance_intvls[ibal][1]=curr_time + time_master_decision+comm_r*load; 
    ibal++;

    //remember new values of load for time curr_time + time_master_decision
    double new_max_load=load_remaining[max_load_red]-load;

    //the other processor has to wait for time comm_r*load until it starts processing
    //to handle this correctly, we add fake load of size (comm_r*load)/a_red
    //available_from value guarantees that we will not do anything (like sending) with this fake load
    double new_min_load=load_remaining[min_load_red]+load  + (comm_r*load)/a_red;

    //update red_keyloads; the above fake load should also be added as load of the first key sent to min_load_red
    for (int i=0;i<how_many_keys;i++)
    {
      if (i==0)      
        red_keyloads[min_load_red].push_back(red_keyloads[max_load_red].back() + (comm_r*load)/a_red);      
      else      
        red_keyloads[min_load_red].push_back(red_keyloads[max_load_red].back());                
      red_keyloads[max_load_red].pop_back();
    }

    if (new_max_load<0) //in case of rounding errors
      new_max_load=0;
    
    //update red_finish
    red_finish[min_load_red]=curr_time + time_master_decision+ a_red*new_min_load; //time for receiving new load is included in the fake part of new_min_load
    red_finish[max_load_red]=curr_time + time_master_decision+ a_red*new_max_load;

    //move to time curr_time + time_master_decision
    move_to_time(load_remaining, curr_time, curr_time + time_master_decision);
    //and correct sizes of load for these 2 reducers
    load_remaining[min_load_red]=new_min_load;
    load_remaining[max_load_red]=new_max_load;
  }

  //after while loop - current time is curr_time and we may have some more load to process
  //find the reducer with greatest load
  int max_load_red=0;
  for (int i=0;i<r;i++)
  {
    if (load_remaining[i]>0)
      red_finish[i]=curr_time + a_red*load_remaining[i];
    if (red_finish[i]>red_finish[max_load_red])
      max_load_red=i;
  }
  //this is the final time    
  total_time=red_finish[max_load_red];

  //compute the second measure for balance costs: length of balancing intervals, taking into account overlapping
  for (int i=0;i<ibal;i++)
  {
    if (i==0 || balance_intvls[i][0]>balance_intvls[i-1][1])
      balance_total_time+=balance_intvls[i][1]-balance_intvls[i][0];
    else if (balance_intvls[i][1]> balance_intvls[i-1][1])
      balance_total_time+=balance_intvls[i][1]-balance_intvls[i-1][1];
  }

  //compute dispersion of relative reducer finish times
  for (int i=0;i<r;i++)
    red_finish[i]/=total_time;
  dispersion_measures(red_finish,r,IQR,stdev);
  return total_time;
}

#pragma endregion

#pragma region divide-keys algorithm
/*
create_partitioning_function() creates partitioning function for given key frequencies
as it is done by the master in divide-keys algorithm;
it returns the length of the sequence representing this function
and the master running time as exec_time 
*/
int create_partitioning_function(double &exec_time)
{
  //create a copy of keyloads and sort it; store also original key numbers (int)
  pair<double, int> *freq=new pair<double, int>[MAX_KEY_NUM];
  for (int i=0;i<key_num;i++)
	freq[i]=make_pair(keyloads[i],i);
  sort(freq,freq+key_num);
  
  exec_time=0;//sorting time will be added at the end (to avoid numerical issues)

  double piece=1.0/r; //keys more frequent than this value are split
  int part_len=0;//length of partitioning function 
  //reducers priority queue used to assign keys
  priority_queue<red_part, vector<red_part>, red_part_compare> q;
  for (int i=0;i<r;i++)
    q.push(red_part(i,0,0));

  exec_time+=a_master*r; //creating reducer queue

  //go from highest to lowest frequency, assign keys to reducers
  for (int i=key_num-1;i>=0;i--)
  {
	bool first=true;
    int who_merges=-1;
    while (freq[i].first>piece) //split it
	{
	  //add the "piece" to the least loaded reducer
	  red_part chosen_red=q.top();
	  q.pop();
	  chosen_red.red_load+=piece;   
	  q.push(chosen_red);
      if (first)//chosen_red will gather this key and perform merging
      {
        first=false;
        who_merges=chosen_red.red_num;
        load_to_merge[chosen_red.red_num]=get_reduced_load(gamma*V*freq[i].first);
      }      
      pieces_to_merge[who_merges]++;
	  //create a new entry in partitioning function
	  part_function[freq[i].second].push_back(make_pair(chosen_red.red_num, piece));
	  part_len++;
	  //and decrease frequency by the "piece"
	  freq[i].first-=piece;

	  exec_time+=a_master*(log(r)/log(2) +1); //time for decrease_key on priority queue with r elements + insert to part_function
	}
	if (freq[i].first>eps)//something still remains
	{
	  //add it to the least loaded reducer
	  red_part chosen_red=q.top();
	  q.pop();
	  chosen_red.red_load+=freq[i].first;   
	  q.push(chosen_red);
	  if(!first)
        pieces_to_merge[who_merges]++;
      //create a new entry in partitioning function
	  part_function[freq[i].second].push_back(make_pair(chosen_red.red_num, freq[i].first));
	  part_len++;

	  exec_time+=a_master*(log(r)/log(2) +1); //time for decrease_key on priority queue with r elements + insert to part_function
	}
  }
  exec_time+=a_master*key_num*log(key_num)/log(2);//initial frequencies sorting time - this is the largest part of exec_time, so we add it at the end (to avoid numerical issues)
  delete [] freq;
  return part_len;
}

/*
divide_keys_method() runs the divide-keys algorithm;
it allows to send data with the same key to different reducers and merge them later
data are preprocessed to compute key frequencies; based on them, the master adjusts the partitioning function
the function is sent to the mappers, processing continues as in do_nothing_method (reference algorithm);
split data with the same key is gathered on the reducer with the smallest number holding this key, and merged 
*/
double divide_keys_method(double &IQR, double &stdev, double &balance_partitioning, double &balance_merge_comm, double &balance_merging)
{
  double total_time=A*V/m;
  //information about key frequencies gathered
  total_time+=m*key_num*comm_master_size*C;
  //and sent to the server,
  //which then computes the partitioning function
  double master_time=0;
  int part_func_length=create_partitioning_function(master_time);
  total_time+=master_time;
  //and the function is sent to the mappers
  total_time+=m*part_func_length*comm_master_size*C;

  //balance costs for partitioning function are reading input for the first time, sending frequencies, computing the function and passing it - everything up to this moment
  balance_partitioning=total_time;

  total_time+=A*V/m;
  //mapping is finished

  //compute load amounts to transfer from one mapper to each reducer using part_function
  double load_to_send[MAX_PROC]={0};
  for (int i=0;i<key_num;i++)
	for (int j=0;j<part_function[i].size();j++)
	{
	  load_to_send[part_function[i][j].first]+=(gamma*V/m)*part_function[i][j].second;
	}

  //overhead for communicating with master (both mappers and reducers)
  total_time+=2*C*m*r*comm_master_size;
  
  //send load to reducers    
  double red_comm_start[MAX_PROC];
  double comm_time=send_data(total_time,load_to_send,red_comm_start,r);  

  //compute reducing finish times; they will be later updated by adding merging times
  double red_time[MAX_PROC]={0};
  for (int i=0;i<r;i++)
  {
    red_time[i]=red_comm_start[i] + sort_time(load_to_send[i]*m) + a_red*(load_to_send[i]*m);
  }

  //what does the reducer do in merging process? 0 - nothing, 1 - send away, 2 - receive and merge
  int red_merge[MAX_PROC]={0};  
  
  double load_left[MAX_PROC]={0};
  vector<comm_merge_event> events;
  for (int i=0;i<key_num;i++)
	if (part_function[i].size()>1) //split key
	{
	  if (red_merge[part_function[i][0].first]!=0)
	  {		
		cout<<"ERROR! Divide_keys_method: reducer has 2 roles in merging process"<<endl;    
		return 0;
	  }
	  red_merge[part_function[i][0].first]=2; //this reducer receives data for merging
	  for (int j=1;j<part_function[i].size();j++)
	  {
		if (red_merge[part_function[i][j].first]!=0)
		{		  
		  cout<<"ERROR! Divide_keys_method: reducer has 2 roles in merging process"<<endl;    
		  return 0;
		}
		red_merge[part_function[i][j].first]=1; //this reducer sends data away
		//compute when the load for this key is ready (reduced); reducers start with reducing split keys
		double ready_time=red_comm_start[part_function[i][j].first] + sort_time(load_to_send[part_function[i][j].first]*m)+a_red*((gamma*V)*part_function[i][j].second);
		//add necessary communication to the events list
		events.push_back(comm_merge_event(part_function[i][j].first, part_function[i][0].first, get_reduced_load((gamma*V)*part_function[i][j].second),ready_time));
	  }
	}

  //process the events list
  int comm_num=0; //how many open connections
  double cur_time=0; //current time
  bool red_busy[MAX_PROC]={false}; //reducer is busy if it communicates or if it still processes the load to be sent
  balance_merge_comm=0;
  while (1)
  {
    double next_time=MAX_REAL; //time left to next event
    //find the next event
    for (int i=0;i<events.size();i++)
    {
      if (events[i].started && events[i].load_left>eps)//compute finish time by current communication speed
      {
        double time_left=events[i].load_left*comm_rate(comm_num);
        if (time_left<next_time)
          next_time=time_left;
      }
      if (!events[i].started && !red_busy[events[i].from] && !red_busy[events[i].to])
      {
        if (events[i].start_from-cur_time <next_time)
          next_time=events[i].start_from-cur_time;
      }
    }
    if (next_time==MAX_REAL) break; //no further events, communications finished
    cur_time+=next_time;

    //move to the next event (cur_time)

    int comm_finished_num=0; //keep track of open channels number
    int found_comm_in_itvl=0;//used to sum intervals with communications
    //finish communications 
    for (int i=0;i<events.size();i++)
    {
      if (events[i].started && events[i].load_left>eps)
      {
        //there was a communication in the current interval of length next_time -> add it to balance cost
        if (!found_comm_in_itvl)
        {
          balance_merge_comm+=next_time;
          found_comm_in_itvl=1;
        }

        double processed_load=next_time/comm_rate(comm_num);     
        if (events[i].load_left - processed_load<eps) //commmunication finished
        {
          events[i].load_left=0;
          red_busy[events[i].from]=false;
          red_busy[events[i].to]=false;
          comm_finished_num++;
          if (cur_time>red_time[events[i].to])//receiver can start merging only after obtaining all data
            red_time[events[i].to]=cur_time;
        }
        else
          events[i].load_left -= processed_load;
      }
    }
    //update comm_num 
    comm_num-=comm_finished_num;
    //if possible, start new communications
    for (int i=0;i<events.size();i++)
    {
      if (!events[i].started && !red_busy[events[i].from] && !red_busy[events[i].to] && events[i].start_from<=cur_time)
      {
        events[i].started=true;
        red_busy[events[i].from]=true;
        red_busy[events[i].to]=true;
        comm_num++; 
      }
    }    
  }
  //communications finished, in red_time[] we have times when merging starts on the reducers
  //now we add merging time on reducers and find schedule length
  balance_merging=0;
  vector<pair<double, double> > intvls; //intervals with merging - compute the length of their sum as balancing cost measure
  double Cmax=0,min_red_time=MAX_REAL;
  for(int i=0;i<r;i++)
  {
    if (load_to_merge[i]>0)
    {
      intvls.push_back(make_pair(red_time[i],  red_time[i]+merge_time(load_to_merge[i],i)));
      red_time[i]+=merge_time(load_to_merge[i],i);
    }
    if (red_time[i]>Cmax)
      Cmax=red_time[i];
    if (red_time[i]<min_red_time)
      min_red_time=red_time[i];
  }

  //compute balancing cost for merging
  if (!intvls.empty())
  {
    sort (intvls.begin(),intvls.end());
    for (int i=0;i<intvls.size();i++)
    {
      if (i==0 || intvls[i].first>intvls[i-1].second)
        balance_merging+=intvls[i].second-intvls[i].first;
      else if (intvls[i].second>intvls[i-1].second)
        balance_merging+=intvls[i].second-intvls[i-1].second;
    }
  }

  //compute dispersion of relative reducer finish times
  for (int i=0;i<r;i++)
    red_time[i]/=Cmax;
  dispersion_measures(red_time,r,IQR,stdev);

  return Cmax;
}
#pragma endregion

int _tmain(int argc, _TCHAR* argv[])
{
  if (argc<2)
  {
    cout << "usage: exefilename inputfile\n";
    return EXIT_FAILURE;
  }  
  cout<<scientific<<setprecision(20);
  dataread(argv[1]);

  double doing_nothing_IQR, doing_nothing_stdev;
  double doing_nothing_time=do_nothing_method(doing_nothing_IQR, doing_nothing_stdev);
  cout<<"method0_time "<<doing_nothing_time<<"\n";

  double divide_keys_IQR, divide_keys_stdev, divide_keys_balance_partitioning, divide_keys_balance_merge_comm, divide_keys_balance_merging;
  double divide_keys_time=divide_keys_method(divide_keys_IQR, divide_keys_stdev, divide_keys_balance_partitioning, divide_keys_balance_merge_comm, divide_keys_balance_merging);  
  cout<<"divide_keys_time "<<divide_keys_time<<"\n";
  
  int k[4]={2,10,100,1000},kval=2;//we use static with k=2 and k=10 only
  double static_IQR[2],static_stdev[2], static_balance[2];
  for (int i=0;i<kval;i++)
  {
    double static_time=static_method(k[i],static_IQR[i],static_stdev[i],static_balance[i]);
    cout<<"static_"<<k[i]<<"_time "<<static_time<<"\n";
  }  
    
  double first_frac[9]={0.5,0.9}; int f=2; //mixed with x=0.5 and x=0.9
  int parts1[4]={10,2,5,1},ip1=1; //we use mixed with k1=k2=10 only
  int parts2[4]={10,2,5,1},ip2=1;
  double mixed_IQR[2], mixed_stdev[2], mixed_balance[2];
  for (int i=0;i<f;i++)
    for (int j=0;j<ip1;j++)
      for (int k=0;k<ip2;k++)
      {
        double mixed_time=mixed_method(first_frac[i],parts1[j],parts2[k],mixed_IQR[i],mixed_stdev[i],mixed_balance[i]);
        printf("mixed_%.2lf",first_frac[i]);
        cout<<"_"<<parts1[j]<<"_"<<parts2[k]<<"_time "<<mixed_time<<"\n";
      }
   
  int new_dynamic_best_transfer_num=0;
  double dynamic_IQR, dynamic_stdev, dynamic_balance_total_time, dynamic_balance_sum_of_times;
  double new_dynamic_time=new_dynamic_method(new_dynamic_best_transfer_num,dynamic_IQR,dynamic_stdev, dynamic_balance_total_time, dynamic_balance_sum_of_times);
  cout<<"dynamic_multi_time "<<new_dynamic_time<<"\n";  
  cout<<"dynamic_multi_transfers "<<new_dynamic_best_transfer_num<<"\n";

  //IQRs
  cout<<"method0_IQR "<<doing_nothing_IQR<<"\n";
  cout<<"divide_keys_IQR "<<divide_keys_IQR<<"\n";
  for (int i=0;i<kval;i++)
    cout<<"static_"<<k[i]<<"_IQR "<<static_IQR[i]<<"\n";
  for (int i=0;i<f;i++)
    for (int j=0;j<ip1;j++)
      for (int k=0;k<ip2;k++)
      {
        printf("mixed_%.2lf",first_frac[i]);
        cout<<"_"<<parts1[j]<<"_"<<parts2[k]<<"_IQR "<<mixed_IQR[i]<<"\n";
      }  
  cout<<"dynamic_multi_IQR "<<dynamic_IQR<<"\n";    

  //stdevs
  cout<<"method0_stdev "<<doing_nothing_stdev<<"\n";
  cout<<"divide_keys_stdev "<<divide_keys_stdev<<"\n";
  for (int i=0;i<kval;i++)
    cout<<"static_"<<k[i]<<"_stdev "<<static_stdev[i]<<"\n";
  for (int i=0;i<f;i++)
    for (int j=0;j<ip1;j++)
      for (int k=0;k<ip2;k++)
      {
        printf("mixed_%.2lf",first_frac[i]);
        cout<<"_"<<parts1[j]<<"_"<<parts2[k]<<"_stdev "<<mixed_stdev[i]<<"\n";
      }  
  cout<<"dynamic_multi_stdev "<<dynamic_stdev<<"\n";

  //load balancing costs
  cout<<"divide_keys_balance_partitioning "<<divide_keys_balance_partitioning<<"\n";
  cout<<"divide_keys_balance_merge_comm "<<divide_keys_balance_merge_comm<<"\n";
  cout<<"divide_keys_balance_merging "<<divide_keys_balance_merging<<"\n";
  cout<<"divide_keys_balance_total "<<divide_keys_balance_partitioning + divide_keys_balance_merge_comm + divide_keys_balance_merging<<"\n";
  for (int i=0;i<kval;i++)
    cout<<"static_"<<k[i]<<"_balance_cost "<<static_balance[i]<<"\n";
  for (int i=0;i<f;i++)
    for (int j=0;j<ip1;j++)
      for (int k=0;k<ip2;k++)
      {
        printf("mixed_%.2lf",first_frac[i]);
        cout<<"_"<<parts1[j]<<"_"<<parts2[k]<<"_balance_cost "<<mixed_balance[i]<<"\n";
      }
  cout<<"dynamic_balance_total_time "<<dynamic_balance_total_time<<"\n";
  cout<<"dynamic_balance_sum_of_times "<<dynamic_balance_sum_of_times<<"\n";
  return 0;
}

