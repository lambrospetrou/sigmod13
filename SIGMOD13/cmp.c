    #include <stdio.h>
    #include <string.h>
    #include <stdlib.h>
    #include <sys/time.h>
     
    struct timeval start, end;
    void timer_start()
    {
      gettimeofday(&start, 0);
    }
    void timer_end(char *what)
    {
      gettimeofday(&end, 0);
      printf("%s %ldusec\n", what, (end.tv_sec*1000000 + end.tv_usec) -
    (start.tv_sec*1000000 + start.tv_usec));
    }
     
     
    int main()
    {
      char *a = "momomomomomomomo";
      char *b = "momomomomomomomo";
     
      long i, cnt = 800000000, size = strlen(a);
     
      timer_start();
      for(i  = 0; i < cnt; i++){memcmp(a, b, size);}
      timer_end("memcmp: ");
     
      timer_start();
      for(i = 0; i < cnt; i++){strncmp(a, b, size);}
      timer_end("strncmp: ");
     
      timer_start();
      for(i = 0; i < cnt; i++){strcmp(a, b);}
      timer_end("strcmp: ");
     
      return 0;
    }
