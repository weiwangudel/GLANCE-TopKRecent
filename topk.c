/* Programmed by wei wang
 * Directed by professor Howie
 *
 * March 21, 2010
 *
 * Find TOP-K Objects in a filesystem
 ***********************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <signal.h>     /* SIGINT */
#include <sys/stat.h>   /* stat() */
#include <unistd.h>
#include <sys/time.h>   /* gettimeofday() */
#include <time.h>		/* time() */
#include <assert.h>     /* assert()*/
#include "level_queue.h"


#define MAX_PERMU 1000000
#define MAX_DRILL_DOWN 1000

/* New struct for saving history */
struct dir_node
{
	long int sub_file_num;
	long int sub_dir_num;

	long int cond_dir_num;
	long int cond_file_num;  /* Number of files that satisfies the conditions */
    double factor;	
	int bool_dir_covered;
	char *dir_abs_path;	 /* absolute path, needed for BFS */			
	struct dir_node *sdirStruct; /* child array dynamically allocated */
};

/* for time statistics */
struct timeval start;
struct timeval end;

double g_select_cond;  /* this records current standard of topk: -10000000 
					    * means the modification time within... such semantics
 						* in units of seconds
 						*/
double est_total;
long int sample_times;
long int qcost = 0;
long int already_covered = 0;
long int newly_covered = 0;

/* program parameters */
long int g_folder;
long int g_file;
long int g_qcost_thresh;  /* smaller than this value, we crawl */
unsigned int g_level_thresh; /* int because level is not so large */
unsigned int g_sdir_thresh; /* para2: sub dir threshold */
double g_percentage; /* how large percent to randomly choose */

/* MAC(modify, access, change) related topk */
int g_k_elem;
time_t g_prog_start_time;

int root_flag = 0; /* only set root factor to 1 in fast_subdir */


int ar[MAX_PERMU];		/* used for permutation */
void swap(int *a, int *b);
void permutation(int size);

struct queueLK level_q;

/********************************************************/
/******* Drill down depth suppose within 200 (level < 200)   *****/
struct dir_node * g_depth_stack[200];
int g_stack_top = 0;
/*****************************************************************/



/***********************functions ********************************/
void CleanExit(int sig);
static char *dup_str(const char *s);
int begin_estimate_from(struct dir_node *rootPtr);
int check_type(const struct dirent *entry);
void fast_subdirs(struct dir_node *curDirPtr);
long int new_count_for_topk(int argc, char* argv[]);

/* why do I have to redefine to avoid the warning of get_current_dir_name? */
char *get_current_dir_name(void);
double floor(double);
int get_eligible_file(const struct dirent *entry);


int main(int argc, char* argv[]) 
{
	/* Get top K */
	if (argc == 10)
	{
		g_k_elem = atoi(argv[9]);
		new_count_for_topk(argc, argv);
	}

	old_count_for_topk(argc, argv);
}
long int new_count_for_topk(int argc, char* argv[]) 
{
	long int i;
	struct dir_node *rootPtr;

	struct dir_node root_dir; /* root directory for estimation */
	long int sample_min = 10000000; /* 10 seconds */
	long int sample_max = 0;
	struct timeval sample_start;
	struct timeval sample_end;
	char *root_abs_name;
	double sum_of_error = 0;
	double sum_of_qcost = 0;
	double sum_of_est = 0;
	
	signal(SIGKILL, CleanExit);
	signal(SIGTERM, CleanExit);
	signal(SIGINT, CleanExit);
	signal(SIGQUIT, CleanExit);
	signal(SIGHUP, CleanExit);

	/* current time as start time of the program */
  	g_prog_start_time = time(NULL);
	
	/* start timer */
	gettimeofday(&start, NULL ); 

	if (argc < 9)
	{
		printf("Usage: %s \n", argv[0]);
		printf("arg 1: drill down times (<100)\n");
		printf("arg 2: file system dir (can be relative path now)\n");
		printf("arg 3: real dirs\n");
		printf("arg 4: real file number\n");
		printf("arg 5: crawl qcost threshold\n");
		printf("arg 6: crawl level threshold\n");
		printf("if only qcost, then level = 0\n");
		printf("if only level, then qcost = 0\n");
		printf("don't use both > 0\n");
		printf("arg 7: sub_dir_num for max(sub_dir_num, **)\n");
		printf("arg 8: percentage chosen, 2 means 50 percent\n"); 
		return EXIT_FAILURE; 
	}
	if (chdir(argv[2]) != 0)
	{
		printf("Error when chdir to %s", argv[2]);
		return EXIT_FAILURE; 
	}
	
	/* this can support relative path easily */
    root_abs_name = dup_str(get_current_dir_name());	

	sample_times = atol(argv[1]);
	assert(sample_times <= MAX_DRILL_DOWN);

	g_folder = atol(argv[3]);
	g_file = atol(argv[4]);
	g_qcost_thresh = atol(argv[5]); 
	g_level_thresh = atoi(argv[6]);
	g_sdir_thresh = atoi(argv[7]);
	g_percentage = atof(argv[8]);

	assert(g_qcost_thresh*g_level_thresh == 0);

	/* initialize the value */
	{
        est_total = 0;
        initQueue(&level_q);
	}

	/* Initialize the dir_node struct */
	rootPtr = &root_dir;
	rootPtr->bool_dir_covered = 0;
	rootPtr->sdirStruct = NULL;
    rootPtr->dir_abs_path = dup_str(root_abs_name);

	int seed;
	srand(seed = (int)time(0));//different seed number for random function

	double * est_array = malloc(sample_times * sizeof (double));
	long int * qcost_array = malloc (sample_times * sizeof (long int ));

	g_select_cond = 1/24 * * 24 * 60 * 60;  /* one day */
	
	/* start estimation */
	for (i=0; i < sample_times; i++)
	{		
		gettimeofday(&sample_start, NULL);
	 	begin_estimate_from(rootPtr);
		est_array[i] = est_total;
		qcost_array[i] = qcost;
		est_total = 0;
	    qcost = 0;		
		rootPtr->bool_dir_covered = 0;
		rootPtr->sdirStruct = NULL;
    	rootPtr->dir_abs_path = dup_str(root_abs_name);
		root_flag = 0;
		gettimeofday(&sample_end, NULL);				
	}

	printf("%s\t", root_abs_name);
	//printf("%d\t%d\t%f\t", g_level_thresh, g_sdir_thresh, g_percentage);


	for (i=0; i < sample_times; i++)
	{	
		sum_of_error += abs(est_array[i] - g_file); 
		sum_of_est += est_array[i];
	}
	
    printf("%.6f\t", sum_of_error/sample_times/g_file);
 
	sum_of_qcost = 0;
	for (i=0; i < sample_times; i++)
	{	
		sum_of_qcost += qcost_array[i]; 
	}

//	printf("%.4f\t", sum_of_qcost/sample_times/g_folder);
	printf("%.4f\n", sum_of_qcost/sample_times/g_folder);
	
  	clearQueue(&level_q);

	/* return est_number of every aggregate query */
	return (long int) (sum_of_est / sample_times); 
	CleanExit (2);
}



/* Before calling begin_estimate_from
 * the dir_node struct has been allocated for root
 */
int n_begin_estimate_from(struct dir_node *rootPtr)
{
    int level = 0;  /* root is in level 1 */
    int clength;
    int vlength;
    struct queueLK tempvec;
    struct dir_node *cur_dir = NULL;
	            
    /* root_dir goes to queue */
    enQueue(&level_q, rootPtr);
    

    /* if queue is not empty */
    while (emptyQueue(&level_q) != 1)    
    {
        level++;
        initQueue(&tempvec);
        /* for all dirs currently in the queue 
         * these dirs should be in the same level 
         */
        for (; emptyQueue(&level_q) != 1; )
        {
            cur_dir = outQueue(&level_q);
			
            /* level ordering, change directory is a big problem 
             * currently save the absolute direcotory of each folder 
             */
            fast_subdirs(cur_dir);
            qcost++;
            est_total = est_total + cur_dir->sub_file_num * cur_dir->factor;
            
            if (cur_dir->sub_dir_num > 0)
            {
                
                vlength = cur_dir->sub_dir_num;

				/* (g_qcost_thresh * g_level_thresh) should always be 0 */
				if ((qcost > g_qcost_thresh) && level > g_level_thresh)
                    clength = min (vlength, 
						max(g_sdir_thresh, floor(vlength/g_percentage)));
                else 
                    clength = vlength;
                
                /* choose clength number of folders to add to queue */
                /* need to use permutation */
                permutation(vlength);
                int i;
                for (i = 0; i < clength; i++)
                {  
                    cur_dir->sdirStruct[ar[i]].factor *= vlength*1.0/clength;
                    enQueue(&tempvec, &cur_dir->sdirStruct[ar[i]]);
                }
            }
        }
		struct dir_node *temp;
        for (; emptyQueue(&tempvec) != 1; )
        {
            temp = outQueue(&tempvec);
            enQueue(&level_q, temp);            
        }  
    }   
}

static char *dup_str(const char *s) 
{
    size_t n = strlen(s) + 1;
    char *t = malloc(n);
    if (t) 
	{
        memcpy(t, s, n);
    }
    return t;
}

void n_fast_subdirs(struct dir_node *curDirPtr) 
{
    long int  sub_dir_num = 0;
    long int  sub_file_num = 0;

    struct dirent **dir_namelist;
	struct dirent **file_namelist;
	
    char *path;
    size_t alloc;
    int total_num;
	int used = 0;

    if (root_flag == 0)
	{	
		curDirPtr->factor = 1.0;
		root_flag = 1;
	}
	/* already stored the subdirs struct before
	 * no need to scan the dir again */
	if (curDirPtr->bool_dir_covered == 1)
	{
		/* This change dir is really important */
		already_covered++;
		chdir(path);

		return;
	}
		
	/* so we have to scan */
	newly_covered++;
    
    path = curDirPtr->dir_abs_path;
	chdir(path);	

	/* root is given like the absolute path regardless of the cur_dir */
    sub_dir_num = scandir(path, &dir_namelist, check_type, 0);
	sub_file_num = scandir(path, &file_namelist, get_eligible_file, 0);

 	alloc = sub_dir_num - 2;
	used = 0;

 	assert(alloc >= 0);

    if (alloc > 0 && !(curDirPtr->sdirStruct
			= malloc(alloc * sizeof (struct dir_node)) )) 
	{
        //goto error_close;
		printf("malloc error!\n");
		exit(-1);
    }
    
    int temp = 0;

	for (temp = 0; temp < alloc; temp++)
	{
		curDirPtr->sdirStruct[temp].sub_dir_num = 0;
		curDirPtr->sdirStruct[temp].sub_file_num = 0;
		curDirPtr->sdirStruct[temp].bool_dir_covered = 0;
	}


	/* scan the namelist */
    for (temp = 0; temp < sub_dir_num; temp++)
    {
		if ((strcmp(dir_namelist[temp]->d_name, ".") == 0) ||
                        (strcmp(dir_namelist[temp]->d_name, "..") == 0))
               continue;
        /* get the absolute path for sub_dirs */
        chdir(dir_namelist[temp]->d_name);
        
   		if (!(curDirPtr->sdirStruct[used].dir_abs_path 
		       = dup_str(get_current_dir_name()))) 
		{
			printf("get name error!!!!\n");
    	}
        curDirPtr->sdirStruct[used].factor = curDirPtr->factor;
		used++;
        chdir(path);		
	}

	sub_dir_num -= 2;

	curDirPtr->sub_file_num = sub_file_num;
	curDirPtr->sub_dir_num = sub_dir_num;
	/* update bool_dir_covered info */
	curDirPtr->bool_dir_covered = 1;
}


int check_type(const struct dirent *entry)
{
    if (entry->d_type == DT_DIR)
        return 1;
    else
        return 0;
}

/* Get MAC timestamp using stat struct */
int n_get_eligible_file(const struct dirent *entry)
{
	struct stat stat_buf;
	double diff;
	
	if (stat(entry->d_name, &stat_buf) != 0)
	{
		printf("stat error!\n");
		exit(-1);		
	}

	diff = difftime(g_prog_start_time, stat_buf.st_atime);

	/* find most recent files */
	if (diff < g_select_cond)
		return 1;	
	else
		return 0;	
}

int n_analyze_aggregate_results()
{
	
}

/************************SIMPLE MATH WORK**********************************/
int min(int a, int b)
{
    if (a < b)
        return a;
    return b;
}

int max(int a, int b)
{
    if (a > b)
        return a;
    return b;
}

void permutation(int size)
{
  int i;
  for (i=0; i < size; i++)
    ar[i] = i;
  for (i=0; i < size-1; i++)
    swap(&ar[i], &ar[Random(i,size)]);
}

void swap(int *a, int *b)
{
    int t;
    t=*a;
    *a=*b;
    *b=t;
}


int Random(int left, int right)
{
	return left + rand() % (right-left);
}


/******************EXIT HANDLING FUNCTION*****************************/
void CleanExit(int sig)
{
    /* make sure everything that needs to go to the screen gets there */
    fflush(stdout);
    gettimeofday(&end, NULL ); 
    

    if (sig != SIGHUP)
        printf("\nExiting...\n");
    else
        printf("\nRestarting...\n");


    puts("\n\n=============================================================");
//	printf("\ndirs newly opened %ld\ndirs already_covered %ld\n",
//			newly_covered, already_covered);
    puts("=============================================================");
    printf("Total Time:%ld milliseconds\n", 
	(end.tv_sec-start.tv_sec)*1000+(end.tv_usec-start.tv_usec)/1000);
    printf("Total Time:%ld seconds\n", 
	(end.tv_sec-start.tv_sec)*1+(end.tv_usec-start.tv_usec)/1000000);

	
	printf("%ld\n", 
    (end.tv_sec-start.tv_sec)*1+(end.tv_usec-start.tv_usec)/1000000);


	exit(0);
}


int old_count_for_topk(int argc, char **argv) 
{
	unsigned int sample_times;
	size_t i;
	struct dir_node *curPtr;
	struct dir_node root;
	
	signal(SIGKILL, CleanExit);
	signal(SIGTERM, CleanExit);
	signal(SIGINT, CleanExit);
	signal(SIGQUIT, CleanExit);
	signal(SIGHUP, CleanExit);

  
	/* start timer */
	gettimeofday(&start, NULL ); 

	if (argc < 3)
	{
		printf("Usage: %s drill-down-times pathname\n",argv[0]);
		return EXIT_FAILURE; 
	}
	if (chdir(argv[2]) != 0)
	{
		printf("Error when chdir to %s", argv[2]);
		return EXIT_FAILURE; 
	}

	sample_times = atoi(argv[1]);
	assert(sample_times <= MAX_INT);

	/* initialize the value */
	{
        est_total = 0;
        est_num = 0;
	}

	/* Initialize the dir_node struct */
	curPtr = &root;
	curPtr->bool_dir_covered = 0;
	curPtr->sdirStruct = NULL;
	
	srand((int)time(0));//different seed number for random function

	
	/* start sampling */
	for (i=0; i < sample_times; i++)
	{
		begin_sample_from(argv[2], curPtr, 1.0);
		//printf("there are on average %.2f files\n", GetResult());
	}

	chdir("/tmp");	  /* this is for the output of gprof */

	/* Exit and Display Statistic */
	CleanExit (2);
	return EXIT_SUCCESS;
}

int random_next(int random_bound)
{
	assert(random_bound < RAND_MAX);
	return rand() % random_bound;	
}

/* to ensure accuray, the root parameter should be passed as 
 * absolute!!!! path, so every return would 
 * start from the correct place
 */
int o_begin_sample_from(
		const char *sample_root, 
		struct dir_node *curPtr,
		double old_prob) 
{
	int sub_dir_num = 0;
	int sub_file_num = 0;
		
    //string curDict = dirstr;
	/* designate the current directory where sampling is to happen */
	char *cur_parent = dup_str(sample_root);
	int bool_sdone = 0;
    //double prob = 1;
	double prob = old_prob;
		
    while (bool_sdone != 1)
    {
		sub_dir_num = 0;
		sub_file_num = 0;

		/* get all sub_dirs struct allocated and organized in to an array
		 * and curPtr's sdirStruct pointer already points to it 
		 * after the execution of the get_all_subdirs ()
		 * current directory is changed to cur_parent, either from
		 * absolute path(the first call of this function under begin_sample_from
		 * or relative path, the subsequent call of that 
		 */
		//get_all_subdirs(cur_parent, curPtr, &sub_dir_num, &sub_file_num);
		fast_subdirs(cur_parent, curPtr, &sub_dir_num, &sub_file_num);

		/* sdirStruct should not be null! */
		/*if (!curPtr->sdirStruct)
		{
			fprintf(stderr, "something went wrong in getting dirs\n");
			return EXIT_FAILURE;
		}    */              
        sub_dir_num = curPtr->sub_dir_num;
		sub_file_num = curPtr->sub_file_num;
		
        est_total = est_total + (sub_file_num / prob);

		if (sub_dir_num > 0)
		{
			prob = prob / sub_dir_num;
			if (prob < old_prob / 10000)
			{
				int i;
				for (i = 0; i < 100; i++)
				{
					begin_sample_from(get_current_dir_name(), 
					    curPtr,
					    prob*100);
				}
				if (((int) old_prob) == 1)
					est_num++;
				chdir(sample_root);
				bool_sdone = 1;
				continue;
			}
			
			int temp = random_next(sub_dir_num);
			cur_parent = dup_str(curPtr->sdirStruct[temp].dir_name);
			curPtr = &curPtr ->sdirStruct[temp];		
		}
		
		/* leaf directory, end the drill down */
        else
        {
			/* backtracking */
			do 
			{
				set_range(g_stack_array[g_stack_top - 1]);
				g_stack_top--;
				          
			} while (g_stack_top > 0);
			
			/* finishing this drill down, set the direcotry back */
			chdir(sample_root);
			bool_sdone = 1;
        }
    } 
    return EXIT_SUCCESS;
}


/* Before the calling of the function, please ensure you have 
 * already CDed to the right place, so the function can directly
 * open the directory for the whole scanning
 * Open dir requires you give either the absolute path, or the relative path
 * to the current directory (which changes all the time)
 */
void get_all_subdirs(   
    const char *path,               /* path name of the parent dir */
    struct dir_node *curPtr,  /* */
	int *sub_dir_num,		        /* number of sub dirs */
	int *sub_file_num)		        /* number of sub files */
{
    DIR *dir;
    struct dirent *pdirent;
    size_t alloc;
	size_t used;
 	struct stat f_ftime;  /* distinguish files from directories */

	/* already stored the subdirs struct before
	 * no need to scan the dir again */
	if (curPtr->bool_dir_covered == 1)
	{
		/* This change dir is really important */
		already_covered++;
		chdir(path);
		printf("cur dir:%s\n", get_current_dir_name());
		return;
	}
		
	/* so we have to scan */
	newly_covered++;
	
	/* root is given like the absolute path regardless of the cur_dir */
    if (!(dir = opendir(path)))
	{
		printf("current dir %s\n", get_current_dir_name());
		printf("error opening dir %s\n", path);
        //goto error;
		exit(-1);
    }

	/* and change to this directory */
	chdir(path);
 
	printf("cur dir:%s\n", get_current_dir_name());
    used = 0;
    alloc = 50;
    if (!(curPtr->sdirStruct
			= malloc(alloc * sizeof (struct dir_node)) )) 
	{
        //goto error_close;
		printf("malloc error!\n");
		exit(-1);
    }

	/* scan the directory */
    while ((pdirent = readdir(dir))) 
	{
		if(strcmp(pdirent->d_name, ".") == 0 ||
				    strcmp(pdirent->d_name, "..") == 0) 
		  continue;
		
		if(stat(pdirent->d_name, &f_ftime) != 0) 
		{   
			printf("stat error\n");			
			return;
		}
		/* currently only deal with regular file and directory 
		 * are there any other kind ?
		 */
		if (S_ISREG(f_ftime.st_mode))
			(*sub_file_num)++;

		/* record the sub direcotry names 
		 * this would contain the hidden files begin with "."
		 */
		else if (S_ISDIR(f_ftime.st_mode)) 
		{
    		if (used + 1 >= alloc) 
			{
        		size_t new = alloc / 2 * 3;
        		struct dir_node *tmp = 
					realloc(curPtr->sdirStruct, new * sizeof (struct dir_node));
        		if (!tmp) 
				{
            		//goto error_free;
					printf("realloc error!\n");
					exit(-1);
        		}
        		curPtr->sdirStruct = tmp;
        		alloc = new;
    		}
    		if (!(curPtr->sdirStruct[used].dir_name = dup_str(pdirent->d_name))) 
			{
        		//goto error_free;
				printf("get name error!!!!\n");
    		}
    		++used;
		}

		/* other non-file, non-dir special files */
		else 
			continue;
	}
	*sub_dir_num = used;

	curPtr->sub_file_num = *sub_file_num;
	curPtr->sub_dir_num = *sub_dir_num;
	/* update bool_dir_covered info */
	curPtr->bool_dir_covered = 1;
    closedir(dir);

	 /* previous approach to prevent memory leak  
error_free:
    while (used--) 
	{
        free(s_dirs[used]);
    }
    free(s_dirs);
 
error_close:
    closedir(dir);
 
error:
    return NULL;   */
}


void o_fast_subdirs(   
    const char *path,               /* path name of the parent dir */
    struct dir_node *curPtr,  /* */
	int *sub_dir_num,		        /* number of sub dirs */
	int *sub_file_num)		        /* number of sub files */
{
    struct dirent **namelist;
    
    size_t alloc;
    int total_num;
	int used = 0;

	/* already stored the subdirs struct before
	 * no need to scan the dir again */
	if (curPtr->bool_dir_covered == 1)
	{
		/* This change dir is really important */
		already_covered++;
		chdir(path);
		printf("cur dir:%s\n", get_current_dir_name());
		return;
	}
		
	/* so we have to scan */
	newly_covered++;
	
	printf("current dir:%s\n", get_current_dir_name());	
    total_num = scandir(path, &namelist, 0, 0);
	
	//rewinddir(path);
	/* root is given like the absolute path regardless of the cur_dir */
    (*sub_dir_num) = scandir(path, &namelist, check_type, 0);
	chdir(path);
	
	*sub_file_num = total_num - *sub_dir_num;
 	alloc = *sub_dir_num - 2;
	used = 0;
	printf("cur dir:%s\n", get_current_dir_name());
 	assert(alloc >= 0);

    if (alloc > 0 && !(curPtr->sdirStruct
			= malloc(alloc * sizeof (struct dir_node)) )) 
	{
        //goto error_close;
		printf("malloc error!\n");
		exit(-1);
    }
    
    int temp = 0;
	/* scan the namelist */
    for (temp = 0; temp < *sub_dir_num; temp++)
    {
		if ((strcmp(namelist[temp]->d_name, ".") == 0) ||
                        (strcmp(namelist[temp]->d_name, "..") == 0))
               continue;
   		if (!(curPtr->sdirStruct[used++].dir_name = dup_str(namelist[temp]->d_name))) 
		{
			printf("get name error!!!!\n");
    	}
		
	}

	*sub_dir_num -= 2;

	curPtr->sub_file_num = *sub_file_num;
	curPtr->sub_dir_num = *sub_dir_num;
	/* update bool_dir_covered info */
	curPtr->bool_dir_covered = 1;

}


