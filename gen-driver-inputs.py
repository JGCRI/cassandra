import os

runid = 'r1i1p1_200601_210012' # seems to be the one runid that is common to all models

gcms = ['CCSM4', 'CESM1_CAM5', 'CSIRO_Mk3_6_0', 'FIO_ESM', 'GFDL_CM3',
        'GFDL_ESM2G', 'GFDL_ESM2M', 'GISS_E2_R']  #  , 'HadGEM2_AO']   <- HadGEM seems to be missing some data
scenarios = ['rcp45', 'rcp60']
#scenarios  = ['rcp60']
pops = ['p80','p85','p90','p95','p10']
#pops  = ['p10']

gcam_scen_str = {'rcp45' : 'c45', 'rcp60' : 'c60'}
wt_str        = {'True' : 'wT', 'False' : 'wF'}
gcm_codes     = {'FIO_ESM' : 'g0', 'CCSM4' : 'g1', 'GISS_E2_R' : 'g2'}

clobber_hydro = 'False'
clobber_disag = 'True'

gcms = ['GISS_E2_R']                # one gcm at a time to prevent hanging on the db query
#gcms = gcm_codes.keys()
water_transfer = 'False'
transfer_file  = '/lustre/data/rpl/GCAMhydro/inputs/water-transfer.csv'
for gcm in gcms:
    for scen in scenarios:
        for pop in pops:
            runname = '%s-%s-%s-%s' % (gcm, scen, pop, wt_str[water_transfer])

            cfgfile = 'run-config/%s.cfg' % runname

            print 'cfgfile = %s' % cfgfile
            
            with open(cfgfile, 'w') as cfg:
                ## write global section (same for all runs)
                cfg.write('[Global]\nModelInterface = /lustre/data/rpl/ModelInterface/ModelInterface.jar\n' +
                          'DBXMLlib = /homes/pralitp/libs/dbxml-2.5.16/install/lib\n\n')

                ## write hydro section
                cfg.write('[HydroModule]\nworkdir = /lustre/data/rpl/GCAMhydro\n' +
                          'inputdir = /lustre/data/CMIP5-data/CMIP5_preprocessed\n' +
                          'outputdir = /lustre/data/rpl/gcam-driver/output/cmip5\n' +
                          'init-storage-file = /lustre/data/rpl/GCAMhydro/inputs/initstorage.mat\n' +
                          'clobber = %s\n'% clobber_hydro) 
                logname = runname + '-hydro-log.txt'
                cfg.write('logfile = /lustre/data/rpl/GCAMhydro/logs/%s\n'%logname)
                cfg.write('gcm = %s\n' % gcm)
                cfg.write('scenario = %s\n' % scen)
                cfg.write('runid = %s\n' % runid)

                ## write GCAM section
                cfg.write('\n[GcamModule]\nexe = /lustre/data/rpl/gcam-ifam-demo/exe/gcam.exe\n' +
                          'logconfig = /lustre/data/rpl/gcam-ifam-demo/exe/log_conf.xml\n' +
                          'clobber = False\n')
                gcam_scen = gcam_scen_str[scen]
                gcam_config = '/lustre/data/rpl/gcam-ifam-demo/exe/configuration-%s%s.xml' % (gcam_scen,pop)
                cfg.write('config = %s\n' % gcam_config)
                gcam_stdout = '/lustre/data/rpl/gcam-ifam-demo/exe/driver-logs/%s-sdout-log.txt' % runname
                cfg.write('logfile = %s\n' % gcam_stdout)

                ## write water disaggregation section.  Most of this is boilerplate, since the module
                ## figures out many of its own filenames
                cfg.write('\n[WaterDisaggregationModule]\n' +
                          'workdir = /lustre/data/rpl/GCAMhydro\n' +
                          'inputdir = /lustre/data/rpl/gcam-driver/input-data\n' +
                          'clobber = %s\n'% clobber_disag) 
                logfile  = '/lustre/data/rpl/GCAMhydro/logs/%s-disag-log.txt' % runname
                cfg.write('logfile = %s\n' % logfile)
                tempdir  = '/lustre/data/rpl/gcam-driver/output/waterdisag/wdtmp-%s'% runname
                try:
                    os.makedirs(tempdir)
                except OSError:
                    pass # os error is normal if the dir already exists.
                outputdir = '/lustre/data/rpl/gcam-driver/output/demo-data/%s'  % runname
                try:
                    os.makedirs(outputdir)
                except OSError:
                    pass # see above.

                cfg.write('tempdir = %s\n' % tempdir)
                cfg.write('outputdir = %s\n' % outputdir)
                cfg.write('scenario = %s\n' % scen)
                cfg.write('water-transfer = %s\n' % water_transfer)
                cfg.write('transfer-file = %s\n' % transfer_file)

                ## write the netcdf production section.
                cfg.write('\n[NetcdfDemoModule]\nmat2nc = /lustre/data/rpl/ifam2/map/data/mat2nc\n')
                ## figure out the metadata descriptors
                rcpval = float(scen[-2:])/10.0
                popval = float(pop[-2:])
                if popval > 15.0:
                    ## 10.0 is encoded as '10'.  Others are encoded as pop*10
                    popval = popval/10.0
                gdpval = 10.0       # not used, but a placeholder is required

                cfg.write('rcp = %f\npop = %f\ngdp = %f\n' % (rcpval, popval, gdpval))

                ## the output file uses the gcam-version of the scenario
                ## designator.  Eventually it will also use a code for the
                ## GCM.
                if gcm_codes.has_key(gcm):
                    gcm_str = gcm_codes[gcm]
                else:
                    gcm_str = gcm+'-'
                outfilename = '/lustre/data/rpl/gcam-driver/output/demo-data/netcdf/%s%s%s%s.nc' % (gcm_str, gcam_scen_str[scen], pop, wt_str[water_transfer])
                cfg.write('outfile = %s\n' % outfilename)


            batchfile = 'run-config/%s.csh' % runname
            with open(batchfile, 'w') as bat:
                bat.write('#PBS -l nodes=1\n#PBS -l walltime=3:00:00:00\n#PBS -Agcam\n\n')
                bat.write('cd /lustre/data/rpl/gcam-driver\ndate\ntap -q matlab\ntap java6\n')
                bat.write('time ./gcam_driver.py ./%s\n' % cfgfile)
                bat.write('date')

            launchstr = 'qsub %s' % batchfile
            print launchstr
            os.system(launchstr)



