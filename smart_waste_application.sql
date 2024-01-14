use assignment5;

drop view if exists app_recycle;
create view App_Recycle as
select wastebin.waste_bin_id, wastebin.x, wastebin.y, wastebin.capacity, "Recycle" as binType
from wastebin, recyclebin
where wastebin.waste_bin_id = recyclebin.waste_bin_id;

drop view if exists app_compost;
create view App_Compost as
select wastebin.waste_bin_id, wastebin.x, wastebin.y, wastebin.capacity, "Compost" as binType
from wastebin, compostbin
where wastebin.waste_bin_id = compostbin.waste_bin_id;

drop view if exists app_landfill;
create view App_Landfill as
select wastebin.waste_bin_id, wastebin.x, wastebin.y, wastebin.capacity, "Landfill" as binType
from wastebin, landfillbin
where wastebin.waste_bin_id = landfillbin.waste_bin_id;

drop view if exists app_bin_info;
create view app_bin_info as
(
	select * from App_Recycle
	union
	select * from app_compost
	union
	select * from app_landfill
);

create or replace view App_Users as
select app_bin_info.waste_bin_id as 'Waste Bin ID', app_bin_info.x, app_bin_info.y, app_bin_info.bintype as 'Type of Bin'
from app_bin_info, loadsensor, loadobservation l1, building
where app_bin_info.waste_bin_id = loadsensor.waste_bin_id
	and loadsensor.sensor_id = l1.sensor_id
    and l1.timestamp = (select l2.timestamp
						from loadobservation l2
                        where loadsensor.sensor_id = l2.sensor_id
							and l2.timestamp < '2019-10-26 13:00:00'
                            and l2.weight < app_bin_info.capacity
						order by l2.timestamp desc
                        limit 1)
	and app_bin_info.x between building.boxlowx and building.boxupperx
    and app_bin_info.y between building.boxlowy and building.boxuppery
group by app_bin_info.waste_bin_id;

select * from App_Users;



-- problem 2

drop view if exists wbinfo;
create view wbinfo as
select w.waste_bin_id, w.x, w.y, lo.timestamp, lo.weight
from wastebin as w, loadsensor as ls, loadobservation as lo
where w.waste_bin_id = ls.waste_bin_id
	and ls.sensor_id = lo.sensor_id;
    
drop view if exists stuinfo;
create view stuinfo as
select s.user_id, s.school_name, loco.timestamp, loco.x, loco.y
from student as s, locationsensor as locs, locationobservation as loco
where s.user_id = locs.user_id
	and locs.sensor_id = loco.sensor_id;

create or replace view Sustainability_Analysts as
select wbinfo.waste_bin_id as 'Waste Bin ID', wbinfo.x, wbinfo.y, stuinfo.school_name as Department, sum(wbinfo.weight) as 'Total Weight'
from wbinfo, stuinfo
where wbinfo.y = stuinfo.y
	and wbinfo.x = stuinfo.x
    and wbinfo.timestamp = stuinfo.timestamp
group by stuinfo.user_id, stuinfo.school_name, wbinfo.waste_bin_id
;

select * from Sustainability_Analysts;



-- problem 3
create or replace view comp as
select cb.waste_bin_id, lo.timestamp as dateT,wb.x,wb.y, 'Compost' binType
from compostbin cb, loadsensor ls, loadobservation lo, wastebin wb
where cb.waste_bin_id = ls.waste_bin_id
	and cb.waste_bin_id = wb.waste_bin_id
	and ls.sensor_id = lo.sensor_id;
    
create or replace view recy as
select rb.waste_bin_id, lo.timestamp as dateT,wb.x,wb.y, 'Recycle' binType
from recyclebin rb, loadsensor ls, loadobservation lo, wastebin wb
where rb.waste_bin_id = ls.waste_bin_id
and rb.waste_bin_id = wb.waste_bin_id
	and ls.sensor_id = lo.sensor_id;
    
create or replace view land as
select lb.waste_bin_id, lo.timestamp as dateT,wb.x,wb.y, 'Landfill' binType
from landfillbin lb, loadsensor ls, loadobservation lo, wastebin wb
where lb.waste_bin_id = ls.waste_bin_id
and lb.waste_bin_id = wb.waste_bin_id
	and ls.sensor_id = lo.sensor_id;
    
drop view if exists stuinfo;
create view stuinfo as
select s.user_id, u.name, loco.timestamp as dateT, loco.x, loco.y
from student as s,user u, locationsensor as locs, locationobservation as loco
where s.user_id = locs.user_id
	and s.user_id = u.user_id
	and locs.sensor_id = loco.sensor_id;
 
 create or replace view comp_count as
select stuinfo.name, date(stuinfo.dateT) as dateT, count(*) as 'Compost', 0 as 'Recycle', 0 as 'Landfill'
from stuinfo, comp
where comp.x = stuinfo.x
and comp.y = stuinfo.y
and stuinfo.dateT = comp.dateT
group by stuinfo.user_id, date(stuinfo.dateT);

 create or replace view recy_count as
select stuinfo.name, date(stuinfo.dateT)as dateT, 0 as 'Compost', count(*) as 'Recycle', 0 as 'Landfill'
from stuinfo, recy
where recy.x = stuinfo.x
and recy.y = stuinfo.y
and stuinfo.dateT = recy.dateT
group by stuinfo.user_id, date(stuinfo.dateT);

create or replace view land_count as
select stuinfo.name, date(stuinfo.dateT)as dateT, 0 as 'Compost', 0 as 'Recycle', count(*) as 'Landfill'
from stuinfo, land
where land.x = stuinfo.x
and land.y = stuinfo.y
and stuinfo.dateT = land.dateT
group by stuinfo.user_id, date(stuinfo.dateT);


create or replace view Facility_Mangaers as
(
	select tab.name as Name, tab.dateT as Day, sum(tab.Compost) as Compost, sum(tab.Recycle) as Recycle, sum(tab.Landfill) as Landfill
    from (select * from comp_count
			union
			select * from recy_count
			union
			select * from land_count) as tab
    group by tab.name, tab.dateT
);

select * from Facility_Mangaers;

-- PROBLEM 3
-- Identifyinf Malfuntioning Sensors

drop trigger if exists ErroneousSensoreDetection_Trigger;

delimiter //
create  trigger ErroneousSensoreDetection_Trigger
before insert 
on loadobservation for each row
begin
	declare lastest_time DATETIME default (select lo.timestamp from loadobservation lo where lo.sensor_id = new.sensor_id order by lo.oid desc limit 1);
	set @latest_weight := (select lo.weight from loadobservation lo where lo.sensor_id = new.sensor_id order by lo.oid desc limit 1);
	if (@latest_weight is null) or ((new.weight - @latest_weight) > 1000 and datediff(new.timestamp , lastest_time) < 2)
		then set new.weight = null;

	end if;
end; //
delimiter ;

delete from loadobservation where sensor_id = 350 and  oid > 50000;

INSERT INTO LoadObservation(sensor_id, oid, Weight, timestamp) VALUES (350, 50001, 15000,
'2017-07-07 20:00:55');
INSERT INTO LoadObservation(sensor_id, oid, Weight, timestamp) VALUES (350, 50002, 15500,
'2017-07-17 22:00:55');
INSERT INTO LoadObservation(sensor_id, oid, Weight, timestamp) VALUES (350, 50003, 17000,
'2017-07-18 20:45:55');
INSERT INTO LoadObservation(sensor_id, oid, Weight, timestamp) VALUES (350, 50004, 17500,
'2017-07-20 20:50:55');

Select * from LoadObservation where sensor_id = 350 and oid > 50000;


-- Trash Violations
drop table if exists TrashViolations;
create table TrashViolations (
	TVID int primary key auto_increment,
    user_id int,
    timestamp DATETIME,
    waste_bin_id int,
    trash_type enum('Recycle', 'Compost', 'Landfill')
    );

drop trigger if exists capacity_check;

delimiter //    
create trigger capacity_check
before insert
on objectrecognitionobservation for each row
begin
	/*create or replace view cap as
	select locationsensor.user_id, objectrecognitionobservation.timestamp, app_bin_info.waste_bin_id, objectrecognitionobservation.trash_type
        from objectrecognitionobservation, objectrecognitionsensor, app_bin_info, locationsensor, locationobservation
        where locationobservation.timestamp = objectrecognitionobservation.timestamp
            and new.timestamp = objectrecognitionobservation.timestamp
			and objectrecognitionsensor.waste_bin_id = app_bin_info.waste_bin_id
            and new.trash_type <> app_bin_info.binType
            and locationobservation.sensor_id = locationsensor.sensor_id
            and new.sensor_id = locationsensor.sensor_id
            and locationobservation.x = app_bin_info.x
            and locationobservation.y = app_bin_info.y
            and objectrecognitionobservation.sensor_id = objectrecognitionsensor.sensor_id;*/
	if exists (
		select *
        from app_bin_info, objectrecognitionsensor
        where new.sensor_id = objectrecognitionsensor.sensor_id
			and objectrecognitionsensor.waste_bin_id = app_bin_info.waste_bin_id
            and new.trash_type <> app_bin_info.binType
		)
        
		then
        insert into TrashViolations(user_id, timestamp, waste_bin_id, trash_type) 
        values (
        (select user_id
        from locationsensor, locationobservation, objectrecognitionsensor
        where locationsensor.sensor_id = locationobservation.sensor_id
        and locationobservation.timestamp = new.timestamp limit 1),
        new.timestamp, 
        (select waste_bin_id
        from objectrecognitionsensor
        where new.sensor_id =  objectrecognitionsensor.sensor_id limit 1), 
        new.trash_type)
        ;
        
	end if;
end; //
delimiter ;

delete from locationobservation where sensor_id = 1 and timestamp = '2017-11-15 14:00:00';
INSERT INTO LocationObservation(sensor_id, oid, timestamp, X, Y) VALUES (1, 100001, '2017-11-15
14:00:00', 5459, 3576);
delete from ObjectRecognitionObservation where sensor_id = 354 and timestamp = '2017-11-15 14:00:00';
INSERT INTO ObjectRecognitionObservation(sensor_id, oid, timestamp, trash_type) VALUES (354,
200001, '2017-11-15 14:00:00', 'LandFill');

Select * from TrashViolations;
