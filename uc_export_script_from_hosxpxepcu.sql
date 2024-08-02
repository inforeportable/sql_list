-- uc_export_script_from_hosxpxepcu 2024-07-31
-- เพื่อประมวลผลข้อมูลการให้บริการเพื่อนำมาใช้ในการติดตามเบื้องต้น


set @s_date = '2022-10-01' ;
set @e_date = '2024-07-31' ;
set @hospital_code = (select  opdconfig.hospitalcode from opdconfig);
select
ovst.vn as hdc_date,
cast(now()  as char)  as export_stamp ,
cast(@s_date as char) as start_date ,
cast(@e_date as char) as stop_date ,
cast(@hospital_code  as char) as hospcode,
cast(lpad(person.person_id,9,0)  as char)  as pid,
cast(if(trim(ovst.hn) = trim(person.cid),'xxx',trim(ovst.hn))  as char)  as hn,
cast(ovst_seq.seq_id  as char)  as seq,
cast(ovst.vstdate  as char)  as date_serv,
cast(provis_instype.pttype_std_code  as char)  as instype,

-- cast( if(trim(replace(ovst.pttypeno,'-','')) = trim(person.cid),'censor',trim(replace(ovst.pttypeno,'-','')))  as char)  as insid,
if(
trim(replace(replace(ovst.pttypeno,'-', ''),' ','')) = trim(person.cid)
,'xxx',
trim(replace(replace(ovst.pttypeno,'-', ''),' ',''))
) 
as insid,

cast(ovst.hospmain  as char)  as main,
cast(left(trim(  replace(replace(replace(opdscreen.cc,'\r\n',' '),char(10),' '),char(13),' ')  ),255) as char)  as chiefcomp,
cast(truncate(sum(opitemrece.qty * opitemrece.cost),2)  as char)   as cost,
cast(truncate(sum(opitemrece.sum_price),2) as char)   as price,
cast(truncate('0.0',2)  as char)   as payprice,
cast(truncate('0.0',2)  as char)   as actualpay,

-- virtual column Change actualpay To pttype_local_type_name(pttype.`name`)
-- cast(pttype.`name` as char)  as actualpay,

cast(ovst.hospsub as char) as hsub,
cast('xxx'  as char)  as cid,
cast(max(case when ovstdiag.diagtype = 1 then ovstdiag.icd10 else null end)  as char)   as pdx,
cast(count(distinct ovstdiag.icd10) as char)   as  count_diag_use,
cast(group_concat(distinct ovstdiag.icd10 order by ovstdiag.diagtype,ovstdiag.icd10) as char)   as  list_diag_code,
cast(group_concat(distinct ovstdiag.doctor order by ovstdiag.diagtype,ovstdiag.icd10) as char)   as list_diagnosis_provider ,
cast(count(distinct doctor_operation.icd9 ) as char)   as count_procedure_use,
cast(group_concat(distinct doctor_operation.icd9 ) as char)   as list_procedure_code,
cast(group_concat(distinct doctor_operation.doctor) as char)  as list_procedure_provider,
cast(count(distinct drugitems.icode) as char)   as count_drug_use,
-- v1 
cast(lower(group_concat(distinct concat(drugitems.name,'|qty=',opitemrece.qty,'|sum_price=',opitemrece.sum_price) )) as char)   as list_drug_name,

-- v2 json hosxp error but standart
-- lower(concat('[',group_concat(distinct concat('{"dname":"', drugitems.name, '", "dqty":"',opitemrece.qty, '", "did":"',drugitems.did,'"}') ),']')) as list_drug_name,

-- v3 json hosxp result no standart show singe quote
-- lower(concat('[',group_concat(distinct concat("{''''dname'''':''''", drugitems.name, "'''', ''''dqty'''':''''",opitemrece.qty, "'''', ''''did'''':''''",drugitems.did,"''''}''''") ),']')) as list_drug_name,

-- v4 json hosxp ok by char(34)
-- replace(
-- lower(concat('[',group_concat(distinct concat('{๐dname๐:๐', drugitems.name, '๐, ๐dqty๐:๐',opitemrece.qty, '๐, ๐did๐:๐',drugitems.did,'๐}') ),']')) 
-- ,'๐',char(34))
-- as list_drug_name,

cast(group_concat(distinct case when drugitems.name is not null then ifnull(opitemrece.doctor,opduser.doctorcode) end ) as char)   as list_drug_provider,
cast(lpad(person.nationality,3,0)  as char)  as nation,
cast(lpad(person.citizenship,3,0)  as char)  as race,
cast(person.pname  as char)   as patient_pname,
cast('xxx'  as char)   as patient_fname,
cast('xxx' as char)   as patient_lname,
cast(person.sex    as char)   as sex,
cast(group_concat(distinct ovstdiag.doctor order by ovstdiag.diagtype,ovstdiag.icd10)   as char)    as provider_id,
cast(group_concat(distinct concat('(',doctor.provider_type_code,')',provider_type.provider_type_name) order by ovstdiag.diagtype,ovstdiag.icd10)   as char)    as providertype,
cast(group_concat(distinct doctor.fname order by ovstdiag.diagtype,ovstdiag.icd10)   as char)    as provider_fname,
cast(group_concat(distinct 'xxx')   as char)    as provider_lname,
cast('0' as char)  as has_dent,
cast('0' as char)  as list_dent_code,
cast('0' as char)  as list_dent_name,
cast(null as char)  as doctor_dm,
cast(now() as char)  as import_datetimestamp
from
	ovst
inner join ovst_seq on ovst.vn = ovst_seq.vn
inner join patient on ovst.hn = patient.hn
inner join person on patient.hn = person.patient_hn
left outer join pttype on ovst.pttype = pttype.pttype
left outer join provis_instype on pttype.nhso_code = provis_instype.code
inner join opdscreen on ovst.vn = opdscreen.vn
left outer join ovstdiag on ovst.vn = ovstdiag.vn
left outer join doctor on ovstdiag.doctor = doctor.code
left outer join provider_type on doctor.provider_type_code = provider_type.provider_type_code
left outer join doctor_operation on ovst.vn = doctor_operation.vn
left outer join er_oper_code on doctor_operation.er_oper_code = er_oper_code.er_oper_code
left outer join doctor as dc_pr on doctor_operation.doctor = dc_pr.code
left outer join opitemrece on ovst.vn = opitemrece.vn
left outer join drugitems on opitemrece.icode = drugitems.icode 
left outer join opduser on opitemrece.staff = opduser.loginname

where
	ovst.vstdate between @s_date
and @e_date
-- and
-- (
-- (left(provis_instype.pttype_std_code,2) = '01' and ovst.hospmain = '10679')
-- or
-- (person.nationality <> '99')
-- )

group by
ovst.vn

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
