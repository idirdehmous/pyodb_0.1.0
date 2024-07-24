      void my_sync_()
      {
#ifdef RS6K
#ifdef PMAPI_P6
        asm("sync 1 " : );
#endif
#endif
      }
