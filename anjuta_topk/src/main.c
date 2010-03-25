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
#define MAX_INT 100000000

struct dir_node
{
	long int sub_file_num;
	long int sub_dir_num;

	double min_age;		/* mtime range */
	double max_age;		
	
	int bool_dir_explored;
	char *dir_name;
	double factor;
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

struct queueLK level_q;
struct queueLK tempvec;

/********************************************************/
/******* Drill down depth suppose within 200 (level < 200)   *****/
struct dir_node *g_depth_stack[200];
int g_stack_top = 0;
double saved_min_age;
double saved_max_age;
double topk_min_age;
double topk_max_age;
/*****************************************************************/



/***********************functions ********************************/
void CleanExit(int sig);
static char *dup_str(const char *s);
int n_begin_estimate_from(struct dir_node *rootPtr);
int check_type(const struct dirent *entry);
void n_fast_subdirs(struct dir_node *curDirPtr);
long int new_count_for_topk(int argc, char* argv[]);

/* why do I have to redefine to avoid the warning of get_current_dir_name? */
char *get_current_dir_name(void);
double floor(double);
int get_eligible_file(const struct dirent *entry);
int eligible_subdirs(struct dir_node sub_dir_ptr);
void record_dir_output_file(struct dir_node *curPtr);
void o_get_subdirs(const char *path, struct dir_node *curPtr);	
int o_begin_sample_from(const char *sample_root, struct dir_node *curPtr);
void set_range(int top);
int n_get_eligible_file(const struct dirent *entry);
void collect_topk();
int old_count_for_topk(int argc, char **argv);

int min(int a, int b);
int max(int a, int b);
int Random(int left, int right);

void swap(int *a, int *b);
void permutation(int size);

int main(int argc, char* argv[]) 
{
	/* Get top K */
	if (argc == 10)
	{
		g_k_elem = atoi(argv[9]);
		new_count_for_topk(argc, argv);
	}

	old_count_for_topk(argc, argv);
	return EXIT_SUCCESS;
}

long int new_count_for_topk(int argc, char* argv[]) 
{
	long int i;
	struct dir_node *rootPtr;

	struct dir_node root_dir; /* root directory for estimation */
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
	rootPtr->bool_dir_explored = 0;
	rootPtr->sdirStruct = NULL;
    rootPtr->dir_abs_path = dup_str(root_abs_name);

	int seed;
	srand(seed = (int)time(0));//different seed number for random function

	double * est_array = malloc(sample_times * sizeof (double));
	long int * qcost_array = malloc (sample_times * sizeof (long int ));

	g_select_cond = 1/24 * 1* 24 * 60 * 60;  /* one day */
	
	/* start estimation */
	for (i=0; i < sample_times; i++)
	{		
		gettimeofday(&sample_start, NULL);
	 	n_begin_estimate_from(rootPtr);
		est_array[i] = est_total;
		qcost_array[i] = qcost;
		est_total = 0;
	    qcost = 0;		
		rootPtr->bool_dir_explored = 0;
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
            n_fast_subdirs(cur_dir);
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
	return EXIT_SUCCESS;
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
	int used = 0;

    if (root_flag == 0)
	{	
		curDirPtr->factor = 1.0;
		root_flag = 1;
	}
	/* already stored the subdirs struct before
	 * no need to scan the dir again */
	if (curDirPtr->bool_dir_explored == 1)
	{
		/* This change dir is really important */
		already_covered++;
		chdir(path);		/** might be a problem here for the count */;
	}
		
	/* so we have to scan */
	newly_covered++;
    
    path = curDirPtr->dir_abs_path;
	chdir(path);	

	/* root is given like the absolute path regardless of the cur_dir */
    sub_dir_num = scandir(path, &dir_namelist, check_type, 0);
	sub_file_num = scandir(path, &file_namelist, n_get_eligible_file, 0);

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
		curDirPtr->sdirStruct[temp].bool_dir_explored = 0;
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
	/* update bool_dir_explored info */
	curDirPtr->bool_dir_explored = 1;
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
	return EXIT_SUCCESS;
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
	char *root_abs_name;
	
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
		printf("topk_min_age\t topk_max_age\n");
		return EXIT_FAILURE; 
	}
	if (chdir(argv[2]) != 0)
	{
		printf("Error when chdir to %s", argv[2]);
		return EXIT_FAILURE; 
	}

	/* support relative path for command options */
	root_abs_name = dup_str(get_current_dir_name());

	sample_times = atoi(argv[1]);
	assert(sample_times <= MAX_INT);

	assert(argv[2] != NULL);
	assert(argv[3] != NULL);
	
	topk_min_age = atof(argv[2]);
	topk_max_age = atof(argv[3]);
	
	/* Initialize the dir_node struct */
	curPtr = &root;
	curPtr->bool_dir_explored = 0;
	curPtr->sdirStruct = NULL;
	
	srand((int)time(0));//different seed number for random function

	
	/* start sampling to get range information */
	for (i=0; i < sample_times; i++)
	{
		o_begin_sample_from(root_abs_name, curPtr);
		curPtr = &root;
	}

	/* get whatever the result of given range is */
	collect_topk();
	
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
		struct dir_node *curPtr) 
{
	int sub_dir_num;
		
	/* designate the current directory where sampling is to happen */
	char *cur_parent = dup_str(sample_root);
	int bool_sdone = 0;
		
    while (bool_sdone != 1)
    {
		o_get_subdirs(cur_parent, curPtr);
		sub_dir_num = curPtr->sub_dir_num;
		
		/* drill down according to reported sub_dir_num */
		if (sub_dir_num > 0)
		{
			/*......deleted divide and conquer.......*/

			/* stack in */
			g_depth_stack[g_stack_top] = curPtr;
			g_stack_top++;

			/* how to do random rejection to boost randomness 
			 * to avoid choosing leaf again?  */
			int temp = random_next(sub_dir_num);				
			cur_parent = dup_str(curPtr->sdirStruct[temp].dir_name);
			curPtr = &curPtr ->sdirStruct[temp];
			
		}
		
		/* leaf directory, end the drill down */
        else
        {
			assert(g_stack_top - 1 >= 0);
			saved_min_age = g_depth_stack[g_stack_top - 1]->min_age;
			saved_max_age = g_depth_stack[g_stack_top - 1]->max_age;
			
			/* backtracking */
			do 
			{
				set_range(g_stack_top - 1);
				g_stack_top--;			/* stack out */
				          
			} while (g_stack_top > 0);
			printf("test2\n");
			/* finishing this drill down, set the direcotry back */
			chdir(sample_root);
			bool_sdone = 1;
        }
    } 
    return EXIT_SUCCESS;
}


void o_get_subdirs(   
    const char *path,               /* path name of the parent dir */
    struct dir_node *curPtr)
{
    struct dirent **namelist;
    
    size_t alloc;
	int used = 0;
	struct stat stat_buf;
	double diff;
	int sub_dir_num;		        /* number of sub dirs */

	/* already stored the subdirs struct before
	 * no need to scan the dir again */
	if (curPtr->bool_dir_explored == 1)
	{
		/* This change dir is really important */
		already_covered++;
		chdir(path);		
		return;
	}
		
	/* so we have to scan */
	newly_covered++;
	
	/* root is given like the absolute path regardless of the cur_dir */
    sub_dir_num = scandir(path, &namelist, check_type, 0);
	
	chdir(path);
	
 	alloc = sub_dir_num - 2;
	used = 0;

    if (alloc > 0 && !(curPtr->sdirStruct
			= malloc(alloc * sizeof (struct dir_node)) )) 
	{
        //goto error_close;
		printf("malloc error!\n");
		exit(-1);
    }
    
    int temp = 0;

	for (temp = 0; temp < alloc; temp++)
	{
		curPtr->sdirStruct[temp].sub_dir_num = 0;
		curPtr->sdirStruct[temp].sub_file_num = 0;
		curPtr->sdirStruct[temp].bool_dir_explored = 0;
	}
	
	/* scan the namelist */
    for (temp = 0; temp < sub_dir_num; temp++)
    {
		if ((strcmp(namelist[temp]->d_name, ".") == 0) ||
                        (strcmp(namelist[temp]->d_name, "..") == 0))
               continue;
   		if (!(curPtr->sdirStruct[used++].dir_name 
		       = dup_str(namelist[temp]->d_name))) 
		{
			printf("get name error!!!!\n");
    	}		
	}

	sub_dir_num -= 2;
	printf("path: %s\n", path);
	/* encounter a new folder, update the [min, max] range */
	if (stat(get_current_dir_name(), &stat_buf) != 0)
	{
		printf("stat error!\n");
		exit(-1);		
	}

	diff = difftime(g_prog_start_time, stat_buf.st_mtime);

	/* arbitrarily set(guess, gamble) min age to be half and make a
	 * arithmetic progression */
	curPtr->min_age = diff / 2;
	curPtr->max_age = diff * 1.5;


	curPtr->sub_dir_num = sub_dir_num;
	
	/* update bool_dir_explored info */
	curPtr->bool_dir_explored = 1;

}

void set_range(int top)
{
	saved_min_age = g_depth_stack[g_stack_top - 1]->min_age;
	saved_max_age = g_depth_stack[g_stack_top - 1]->max_age;

	if (g_depth_stack[top]->min_age > saved_min_age)
		g_depth_stack[top]->min_age = saved_min_age;

	if (g_depth_stack[top]->max_age < saved_max_age)
		g_depth_stack[top]->max_age = saved_max_age;

	/* save the [min, max] for parent use */
	saved_min_age = g_depth_stack[top]->min_age;
	saved_max_age = g_depth_stack[top]->max_age;
	
}

/* traverse to collect topk */
void collect_topk(struct dir_node *rootPtr)
{
    int level = 0;  /* root is in level 1 */
    int clength;
    int vlength;

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
			
			/* find the eligible dirs and files for record (into queue)
			 * and output (to display
			 */           
			record_dir_output_file(cur_dir);
            
            
            if (cur_dir->sub_dir_num > 0)
            {
                
                vlength = cur_dir->sub_dir_num;

				/* (g_qcost_thresh * g_level_thresh) should always be 0 */
				if ((qcost > g_qcost_thresh) && level > g_level_thresh)
                    clength = min (vlength, 
						max(g_sdir_thresh, floor(vlength/g_percentage)));
                else 
                    clength = vlength;
                
                int i;
                for (i = 0; i < clength; i++)
                {  
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


void record_dir_output_file(struct dir_node *curPtr)
{
	int i;
    struct dirent **dir_namelist;
	struct dirent **file_namelist;
	long int sub_dir_num;
	long int sub_file_num;
	long int used;
	long int alloc;
	
	/* if the directory has been explored, meaning the subdir struct are
	 * there for use */	
	if (curPtr->bool_dir_explored == 1)
	{
		/* choose the appropriate subdir for topk collection */
		for (i = 0; i < curPtr->sub_dir_num; i++)
		{
			if (eligible_subdirs(curPtr->sdirStruct[i]) == 1)
				enQueue(&tempvec, &curPtr->sdirStruct[i]);
		}		
					
	}

	/* this directory has never been drilled down, we need to explore 
	 * its sub_dirs (malloc for future traverse) and put it to the queue  
	 */
	else
	{
		sub_dir_num = scandir(curPtr->dir_abs_path, 
		                      &dir_namelist, check_type, 0);
		
 		alloc = sub_dir_num - 2;
		used = 0;

  		if (alloc > 0 && !(curPtr->sdirStruct
			= malloc(alloc * sizeof (struct dir_node)) )) 
		{
		    //goto error_close;
			printf("malloc error!\n");
			exit(-1);
		}
		
		int temp = 0;

		for (temp = 0; temp < alloc; temp++)
		{
			curPtr->sdirStruct[temp].sub_dir_num = 0;
			curPtr->sdirStruct[temp].sub_file_num = 0;
			curPtr->sdirStruct[temp].bool_dir_explored = 0;
		}
	
		/* scan the namelist */
		for (temp = 0; temp < sub_dir_num; temp++)
		{
			if ((strcmp(dir_namelist[temp]->d_name, ".") == 0) ||
		                    (strcmp(dir_namelist[temp]->d_name, "..") == 0))
		           continue;
	   		if (!(curPtr->sdirStruct[used].dir_name 
				   = dup_str(dir_namelist[temp]->d_name))) 
			{
				printf("get name error!!!!\n");
			}	

			/* also queue in */
			enQueue(&tempvec, &curPtr->sdirStruct[used]);
			used++;
		}

		sub_dir_num -= 2;
		

		curPtr->sub_dir_num = sub_dir_num;
	
		/* update bool_dir_explored info */
		curPtr->bool_dir_explored = 1;		
		
	}
	
	/* check all the file's modification time under the curPtr->dirname
	 * and output those are eligible (within the topk range
	 */
	sub_file_num = scandir(curPtr->dir_abs_path, &file_namelist,
	                       n_get_eligible_file, 0);
	for (i = 0; i < sub_file_num; i++)
	{
		printf("%s/%s\n", curPtr->dir_abs_path, file_namelist[i]->d_name);		
	}
	
}

int eligible_subdirs(struct dir_node sub_dir)
{
	if ((sub_dir.max_age < topk_min_age) ||
		(topk_max_age < sub_dir.min_age))
	{
		return 0;		
	}
	
	return 1;
}

