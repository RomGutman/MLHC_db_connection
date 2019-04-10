# /usr/bin/python

from load_data import LoadData


if __name__ == '__main__':
    query_mimic = """
        select distinct i.label
        from mimiciii.outputevents as o 
        join mimiciii.d_items as i on i.itemid = o.itemid 
        limit 5;
        """

    load_d = LoadData('config_template.ini', 'mimic')

    print(load_d.query_db(query_mimic))
    load_d.query_and_save(query_mimic)

    query_eicu  = """
    SELECT DISTINCT
        t.patientunitstayid AS pid
        ,t.treatmentid AS tid
        ,t.treatmentoffset AS toffset
    FROM
        eicu_crd.treatment t 
    WHERE 
        t.treatmentstring LIKE %(treat_string)s
    ORDER BY t.patientunitstayid limit 5;"""

    load_d2 = LoadData('config_template.ini', 'eicu')
    params = {'treat_string': 'pulmonary|ventilation and oxygenation|prone position'}
    print(load_d2.query_db(query_eicu, params=params))

