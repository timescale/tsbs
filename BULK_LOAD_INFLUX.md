Example usage (from a shell session):

# bulk_load_influx populates an influxd instance (there are many CLI options):
$ time bulk_load_influx --points=1000000 --batch-size=10000 --std-devs=1,2 --measurements=8 --tag-keys=4 --tag-values=4 --fields=2
wrote 1000000 points across 8 measurements (126.94MB)
  measurement_kfvqe: 125000 points. tag pairs: 16, fields: 2, stddevs: [1 2], means: [0 0]
  measurement_vubus: 125000 points. tag pairs: 16, fields: 2, stddevs: [1 2], means: [0 0]
  measurement_amzpm: 125000 points. tag pairs: 16, fields: 2, stddevs: [1 2], means: [0 0]
  measurement_ukslo: 125000 points. tag pairs: 16, fields: 2, stddevs: [1 2], means: [0 0]
  measurement_pjjql: 125000 points. tag pairs: 16, fields: 2, stddevs: [1 2], means: [0 0]
  measurement_udayh: 125000 points. tag pairs: 16, fields: 2, stddevs: [1 2], means: [0 0]
  measurement_bcrow: 125000 points. tag pairs: 16, fields: 2, stddevs: [1 2], means: [0 0]
  measurement_qxxfo: 125000 points. tag pairs: 16, fields: 2, stddevs: [1 2], means: [0 0]

$ influx --database=benchmark_db
Visit https://enterprise.influxdata.com to register for updates, InfluxDB server management, and monitoring.
Connected to http://localhost:8086 version 0.12.0-HEAD
InfluxDB shell 0.12.0-HEAD

# data is written to a generated set of measurements:
> show measurements
name: measurements
------------------
name
measurement_amzpm
measurement_bcrow
measurement_kfvqe
measurement_pjjql
measurement_qxxfo
measurement_udayh
measurement_ukslo
measurement_vubus

> select * from measurement_amzpm limit 10
name: measurement_amzpm
-----------------------
time    field_mtmne     field_uezhl     tag_htmhq       tag_nplnj       tag_pivmr       tag_vqdxe
100     0.749836        2.190255        ejdhr           oozwe           eausf           ojqnb
200     0.825274        -1.080303       mncxs           izmsd           eausf           oqggi
300     1.065724        -0.583016       mncxs           izmsd           qrjbq           tofqy
400     0.398803        0.431377        kaewz           fhwzg           jklox           oqggi
500     -2.08698        0.003029        mncxs           izmsd           jklox           ojqnb
600     3.401933        -0.0298         mncxs           fhwzg           hdfgz           puhbd
700     -0.61042        0.419517        mncxs           izmsd           jklox           ojqnb
800     0.949621        -1.660237       kaewz           izmsd           hdfgz           tofqy
900     -2.248881       1.098165        kaewz           wrvue           qrjbq           tofqy
1000    1.617758        -0.637884       kaewz           izmsd           jklox           puhbd

# means and standard deviations match what was specified (default mean is zero):
> select mean(field_mtmne), stddev(field_mtmne), mean(field_uezhl), stddev(field_uezhl) from measurement_amzpm
name: measurement_amzpm
-----------------------
time    mean                    stddev                  mean_1                  stddev_1
0       0.0020936815039999676   1.995824079833926       0.0027464951040000124   1.0026801922462438

# 4 tag values are used for each tag key:
> show tag values from measurement_amzpm with key = tag_htmhq
name: measurement_amzpm
-----------------------
key             value
tag_htmhq       ejdhr
tag_htmhq       mncxs
tag_htmhq       kaewz
tag_htmhq       cmpvw
