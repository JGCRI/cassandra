import os

hist_runid = 'r1i1p1_195001_200512'
fut_runid  = 'r1i1p1_200601_210012'

#gcms = ['GFDL_CM3', 'HadGEM2_ES', 'IPSL_CM5A_LR']
gcms = ['GFDL_CM3', 'IPSL_CM5A_LR'] # HadGEM2 needs special treatment
scenarios = ['rcp45','rcp85']

for gcm in gcms:
    for scen in scenarios:
        runname = '%s-%s'%(gcm,scen)

        cfgfile = 'lamp-inputs/%s.cfg' % runname
        print 'cfgfile = %s' % cfgfile

        with open(cfgfile, 'w') as cfg:
            ## write global section (same for all runs)
            cfg.write('[Global]\nModelInterface = /lustre/data/rpl/ModelInterface/ModelInterface.jar\n' +
                      'DBXMLlib = /homes/pralitp/libs/dbxml-2.5.16/install/lib\n\n')

            ## write historical hydro section
            cfg.write('[HistoricalHydroModule]\nworkdir = /lustre/data/rpl/GCAMhydro\n' +
                      'inputdir = /lustre/data/CMIP5-data/CMIP5_preprocessed\n' +
                      'outputdir = /lustre/data/rpl/gcam-driver/output/LAMP\n'+
                      'clobber = False\n')
            logfilestr = 'logfile = /lustre/data/rpl/GCAMhydro/logs/%s-historical-hydro.txt\n' % gcm
            cfg.write(logfilestr)
            gcmstr = 'gcm = %s\n' % gcm
            cfg.write(gcmstr)
            runidstr = 'runid = %s\n' % hist_runid
            cfg.write(runidstr)

            ## write future hydro section
            cfg.write('\n[HydroModule]\nworkdir = /lustre/data/rpl/GCAMhydro\n' +
                      'inputdir = /lustre/data/CMIP5-data/CMIP5_preprocessed\n' +
                      'outputdir = /lustre/data/rpl/gcam-driver/output/LAMP\n' +
                      'clobber = False\n')
            logfilestr = 'logfile = /lustre/data/rpl/GCAMhydro/logs/%s-%s-future-hydro.txt\n' % (gcm, scen)
            cfg.write(logfilestr)
            cfg.write(gcmstr)   # same as the historical
            scenstr = 'scenario = %s\n' % scen
            cfg.write(scenstr)
            runidstr = 'runid = %s\n' % fut_runid
            cfg.write(runidstr)

        batchfile = 'lamp-inputs/%s.csh' % runname
        with open(batchfile, 'w') as bat:
            bat.write('#PBS -l nodes=1\n#PBS -l walltime=3:00:00:00\n#PBS -Agcam\n\n')
            bat.write('cd /lustre/data/rpl/gcam-driver\ndate\ntap -q matlab\ntap java6\n')
            bat.write('time ./gcam_driver.py ./%s\n' % cfgfile)
            bat.write('date')
            
            launchstr = 'qsub %s' % batchfile
            print launchstr
            os.system(launchstr)
