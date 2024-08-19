-- uc_export_script_from_hosxpxepcu 2024-08-19 13:34:22
-- update [2024-08-19 13:34:22]
-- เพื่อประมวลผลข้อมูลการให้บริการเพื่อนำมาใช้ในการติดตามเบื้องต้น

set @s_date := '2022-10-01' ;
set @e_date := '2024-07-31' ;
set @hospital_code := (select opdconfig.hospitalcode from opdconfig);
set @dbversion := (select version()) ;
set @sqlversion:= '[2024-08-19 13:34:2]' ;

select
-- service data
	cast(ovst.vn as char) as hdc_date,
	cast(now() as char)  as export_stamp ,
	cast(@s_date as char) as start_date ,
	cast(@e_date as char) as stop_date ,
	cast(@hospital_code as char) as hospcode,
	cast(ovst_seq.seq_id as char) as seq,
	cast(ovst.vstdate as char) as date_serv,
-- patient data
	cast(lpad(person.person_id,9,0) as char) as pid,
	cast(if(trim(ovst.hn) = trim(person.cid),'xxx',trim(ovst.hn)) as char) as hn,
  cast('xxx' as char) as cid,
	cast(person.pname as char)  as patient_pname,
	@dbversion as patient_fname,
	@sqlversion as patient_lname,
	cast(person.sex as char) as sex,
	cast(lpad(person.nationality,3,0) as char) as nation,
	cast(lpad(person.citizenship,3,0) as char) as race,
-- right of treatment
 cast(provis_instype.pttype_std_code as char) as instype,
	concat_ws('|',pttype.`name`,
	if(trim(replace(replace(ovst.pttypeno,'-', ''),' ','')) = trim(person.cid)
	,'xxx',
	trim(replace(replace(ovst.pttypeno,'-', ''),' ',''))
	)) as insid,
	cast(ovst.hospmain as char) as main,
	cast(ovst.hospsub as char) as hsub,
-- screen and money
  cast(left(trim(replace(replace(replace(opdscreen.cc,'\r\n',' '),char(10),' '),char(13),' ')),255) as char) as chiefcomp,
	cast(vt_od.cost as char) as cost,
	cast(vt_od.price as char) as price,
	cast(vt_od.sum_drug_unitprice as char) as pay_price,
	cast(vt_od.sum_drug_unitcost as char) as actualpay ,
-- group diag
	cast(vt_d.count_diag as char) as count_diag_use,
  cast(vt_d.list_diag_code as char) as list_diag_code,
	cast(vt_d.pdx_list_code as char) as pdx,
	cast(concat(vt_d.pdx_list_provider_code,'-',vt_d.pdx_list_doctor_code) as char) as list_diagnosis_provider ,
	cast(vt_d.list_diag_doctor_code as char) as provider_id,
	cast(vt_d.list_diag_doctor_provider_code_name as char) as providertype,
	cast(vt_d.list_diag_doctor_code_name as char) as provider_fname,
	cast(group_concat(distinct 'xxx') as char) as provider_lname,
-- group procedure
  cast(vt_p.count_procedure as char) as count_procedure_use,
	cast(vt_p.list_procedure as char) as list_procedure_code,
	cast(vt_p.list_procedure_doctor_name as char) as list_procedure_provider,
-- group drug
  cast(vt_od.count_drug as char) as count_drug_use,
	cast(vt_od.list_drug as char) as list_drug_name,
  cast(vt_od.list_drug_doctor_name as char) as list_drug_provider,
-- special check
	cast('0' as char) as has_dent,
	cast('0' as char) as list_dent_code,
	cast('0' as char) as list_dent_name,
	cast(vt_d.check_has_doctor_dm as char) as doctor_dm,
	cast(now() as char) as import_datetimestamp	
from
	ovst
	inner join ovst_seq on ovst.vn = ovst_seq.vn
	inner join patient on ovst.hn = patient.hn
	inner join person on patient.hn = person.patient_hn
	left outer join pttype on ovst.pttype = pttype.pttype
	left outer join provis_instype on pttype.nhso_code = provis_instype.code
	inner join opdscreen on ovst.vn = opdscreen.vn
-- vt_d : diagnosis
left outer join
(
select
	ovstdiag.vn,
	ovstdiag.hn,
	ovstdiag.vstdate,
	group_concat(distinct case when ovstdiag.diagtype = 1 then ovstdiag.icd10 else null end order by ovstdiag.icd10 asc) as pdx_list_code,
  group_concat(distinct case when ovstdiag.diagtype = 1 then concat('[',provider_type.provider_type_code,']',provider_type.provider_type_name) else null end order by ovstdiag.icd10 asc) as pdx_list_provider_code,		
  group_concat(distinct case when ovstdiag.diagtype = 1 then concat('[',doctor.`code`,']',doctor.fname) else null end order by ovstdiag.icd10 asc) as pdx_list_doctor_code,
	group_concat(distinct ovstdiag.icd10 order by ovstdiag.diagtype asc) as list_diag_code,
	group_concat(distinct ovstdiag.doctor order by ovstdiag.diagtype asc) as list_diag_doctor_code,
	group_concat(distinct concat('[',provider_type.provider_type_code,']',provider_type.provider_type_name) order by ovstdiag.diagtype asc) as list_diag_doctor_provider_code_name,
	group_concat(distinct concat('[',doctor.`code`,']',doctor.fname) order by ovstdiag.diagtype asc) as list_diag_doctor_code_name,
	count(distinct ovstdiag.icd10) as count_diag,
	max(case when ovstdiag.diagtype = 1 then 1 else 0 end) as check_has_pdx,
  max(case when (left(ovstdiag.icd10,3) between 'e10' and 'e14') and left(provider_type.provider_type_code,2) ='01' then 1 else 0 end) as check_has_doctor_dm
from
	ovstdiag
left outer join doctor on ovstdiag.doctor = doctor.`code`
left outer join provider_type on doctor.provider_type_code = provider_type.provider_type_code
where
	ovstdiag.vstdate between @s_date and @e_date
group by
	ovstdiag.vn
order by
	ovstdiag.vn
) as vt_d on ovst.vn = vt_d.vn
-- vt_od : opitemrece,drug
left outer join
(
select
	opitemrece.vn,
	opitemrece.hn,
	opitemrece.vstdate,
	truncate(sum(opitemrece.qty * opitemrece.cost),2) as cost,
	truncate(sum(opitemrece.sum_price),2) as price,
	count(distinct opitemrece.icode) as count_all,
	count(distinct drugitems.icode) as count_drug,
	truncate(sum(case when drugitems.icode is not null then opitemrece.qty*drugitems.unitcost else 0 end),2) sum_drug_unitcost,
	truncate(sum(case when drugitems.icode is not null then opitemrece.qty*drugitems.unitprice else 0 end),2) sum_drug_unitprice,
	group_concat(distinct lower(concat(drugitems.name,'|qty=',opitemrece.qty,'|sum_price=',opitemrece.sum_price)) ) as list_drug,
	group_concat(distinct case when drugitems.name is not null then ifnull(opitemrece.doctor,opduser.doctorcode) end ) list_drug_doctor_code,
	group_concat(distinct case when drugitems.name is not null then concat('[',doctor.`code`,']',doctor.fname) else null end ) as list_drug_doctor_name,
	group_concat(distinct case when drugitems.name is not null then concat('[',provider_type.provider_type_code,']',provider_type.provider_type_name) else null end) as list_drug_provider_code_name
from
	opitemrece
left outer join drugitems on opitemrece.icode = drugitems.icode
left outer join opduser on opitemrece.staff = opduser.loginname
left outer join doctor on opduser.doctorcode = doctor.`code`
left outer join provider_type on doctor.provider_type_code = provider_type.provider_type_code
where
	opitemrece.vstdate between @s_date and @e_date
group by
	opitemrece.vn
) as vt_od on ovst.vn = vt_od.vn
-- vt_p : dtmain,dttm,doctor_operation,er_oper_code,er_oper_code_area
left outer join
(
select 
	vtp.vn,
	vtp.vstdate,
	group_concat(distinct concat('[',doctor.`code`,']',doctor.fname) ) as list_procedure_doctor_name,
	group_concat(distinct concat('[',provider_type.provider_type_code,']',provider_type.provider_type_name)) as list_procedure_provider_code_name,
	count(distinct vtp.procedure_code) as count_procedure,
	group_concat(distinct vtp.procedure_code) as list_procedure,
	max(case when vtp.source = 'dental' then 1 else 0 end) as check_procedure_dental,
	max(vtp.check_dent_treatment) as check_dent_treatment
from
(
-- dtmain,dttm [group dental]
select
	dtmain.vn,
	dtmain.vstdate,
	dtmain.doctor as doctor,
	ifnull(dttm.icd10tm_operation_code,dttm.icd9cm) as procedure_code,
	case when dttm.treatment = 'y' then 1 else 0 end as check_dent_treatment,
	'dental' as source
from 
	dtmain
inner join dttm on dtmain.tmcode = dttm.`code` 
where
	date(dtmain.vstdate) between @s_date and @e_date
union all
-- doctor_operation,er_oper_code,er_oper_code_area [group doctor_operation]
select
	doctor_operation.vn as vn,
	date(doctor_operation.begin_date_time) as vstdate,
	doctor_operation.doctor,
	ifnull(
	ifnull(
	CASE WHEN BIN(er_oper_code.icd9cm) IS NULL THEN NULL ELSE er_oper_code.icd9cm END,
	CASE WHEN BIN(doctor_operation.icd9) IS NULL THEN NULL ELSE doctor_operation.icd9 END
)
	,
	ifnull(
	CASE WHEN BIN(er_oper_code.icd10tm) IS NULL THEN NULL ELSE er_oper_code.icd10tm END,
	CASE WHEN BIN(er_oper_code_area.icd10tm_operation_code) IS NULL THEN NULL ELSE er_oper_code_area.icd10tm_operation_code END
	)
	) as procedure_code,
	'0' as check_dent_treatment,
	'eroper' as source
from
	doctor_operation
left outer join er_oper_code on doctor_operation.er_oper_code = er_oper_code.er_oper_code
left outer join er_oper_code_area on doctor_operation.er_oper_code_area_id = er_oper_code_area.er_oper_code_area_id
where
	date(doctor_operation.begin_date_time) between @s_date and @e_date
) as vtp
left outer join doctor on vtp.doctor = doctor.`code`
left outer join provider_type on doctor.provider_type_code = provider_type.provider_type_code
where 
	vtp.procedure_code is not null
group by
	vtp.vn
order by vtp.vn
) as vt_p on ovst.vn = vt_p.vn

where
	ovst.vstdate between @s_date and @e_date
group by
	ovst.vn

-- Update Log

-- update [2024-08-19 13:34:22]
-- * ปรับ list_procedure_code : แก้อาการรหัสไม่ออก เพราะเจอข้อความ '' แก้ปัญหาด้วย case when bin(column) IS NULL THEN NULL ELSE column END

-- update [2024-08-16 09:45:57]
-- * ปรับ patient_fname : จากเดิม 'xxx' ประยุกต์เป็น @dbversion := (select version()) ; เพื่อสำรวจก่อน ทำ SQL ต่อไป
-- * ปรับ patient_lname : จากเดิม 'xxx' ประยุกต์เป็น @sqlversion เพื่อแสดงคำสั่ง SQL

-- update 2024-08-13 [2024-08-13 08:44:30]
-- * ปรับ จัดลำดับ column ตามเนื้อหา
-- * สร้าง Sub Query ตามกลุ่มเนื้อหา เพื่อลดปัญหาใช้ Memory และ Bug เวลา Join ข้อมูล Multiple Record
-- * ปรับ vt_od : opitemrece,drug กลุ่ม ยา และ ค่าใช้จ่าย 
-- * ปรับ vt_d : diagnosis วินิจฉัย
-- * ปรับ vt_p : dtmain,dttm,doctor_operation,er_oper_code,er_oper_code_area หัตถการทันตกรรม และ หัตถการ

-- Update 2024-08-07 [2024-08-07 09:06:07]
-- * ปรับ จัดลำดับตาม IFNULL ดังนี้ ICD-9-CM รองลงมาเป็น ICD-10-TM
-- * มาตรฐาน 43 แฟ้ม เวอร์ชั่น 2.4 รหัสหัตถการ ตารางราง PROCEDURE_OPD : Description : รหัสมาตรฐาน ICD-9-CM หรือ ICD-10-TM (รหัสหัตถการ) 

-- Update 2024-08-06
-- * subquery ตารางหัตถการ เพื่อให้ รหัสหัตถการออกมากทั้งหมด ทั้งหัตถการทั่วไป และ ทันตกรรม (subquery call procedure list)
-- * ปรับ icd9 หัตถการทั่วไป : ดึงมาจากหลาย Column ตามลำดับหากไม่เจอให้ดูที่ Column ถัดไปจนกว่าจะไม่มี เน้น ICD10TM
-- * ปรับ ลำดับ er_oper_code_area.icd10tm_operation_code, doctor_operation.icd9,er_oper_code.icd10tm,er_oper_code.icd9cm
-- * ปรับ icd9 ทันตกรรม : เรียงลำดับ dttm.icd10tm_operation_code,dttm.icd9cm เน้น ICD10TM

-- Update 2024-08-05
-- * subquery ตารางหัตถการ เพื่อให้ รหัสหัตถการออกมากทั้งหมด ทั้งหัตถการทั่วไป และ ทันตกรรม (subquery call procedure list)
-- * ปรับ count_procedure_use : ตาราง subquery cvt.count_procedure
-- * ปรับ list_procedure_code : ตาราง subquery cvt.list_procedure_code
-- * ปรับ list_procedure_provider : ตาราง subquery cvt.list_procedure_doctor

-- Update 2024-07-31
-- * ปรับ hdc_date : ใช้ VN แทน เพราะไม่สามาถนำข้อมูลจาก HDC มาใช้ได้
-- * ปรับ chiefcomp : Line Break,New line ในข้อความโดยใช้ '\r\n',char(10),char(13)
-- * ปรับ count_drug_use : เพิ่ม ราคาขาย '|sum_price='
-- * ปรับ insid : ตรวจสอบ หากเลขตรงกับที่ประชาชน ให้ปกปิดข้อความเป็น 'xxx' โดยเอาข้อความ ' 'และ'-' ออกจากข้อความก่อน
-- * ปรับ hn : ตรวจสอบ หากเลขตรงกับที่ประชาชน ให้ปกปิดข้อความเป็น 'xxx' 
-- * count_drug_use : v4 ยังไม่ใช้ เนื่องจาก ยังไม่มีการปรับโครงสร้างฐานข้อมูล ของ เจ้าหน้าที่ตรวจสอบ เพื่อให้ รองรับ json 
-- * count_drug_use : v4 ทำเพื่อ เนื่องจาก โปรแกรม hosxp ไม่รองรับ double quote ในการแสดงผล
-- * count_drug_use : v4 มีการใช้ข้อความภาษาไทยแทน double quote โดยเป็นเลขศูนย์ไทย "๐" และแทนที่ด้วย char(34)
-- * count_drug_use : v4 ทำเป็น json array เพื่อให้รองรับ ฐานข้อมูลในปี 2568
-- Data Process Send
-- * ① ข้อมูล Excel จาก รพ.สต.xls,xlsx : HosxpXE PCU ประมวลผล
-- * ② Private Google Drive : Store ไฟล์ Excel
-- * ③ Private ฐานข้อมูล MariaDB : ปรับข้อมูลให้ตรงรูปแบบ
-- * ④ Private ฐานข้อมูล SQLite : ตรวจสอบข้อมูลแยกบริการ
