select *,
  CONCAT(deviceID, '-', platform, '-', osVer, '-', deviceModel) as deviceFingerprint
from transaction;


-- check if many users are using the same device fingerprint
select userID,
	count(*) as txn_count,
    sum(case when userChargeAmount = 0 then 1 else 0 end) as zero_amount_txns,
    min(reqDate) as first_txn,
    max(reqDate) as last_txn
from transaction
group by userID
having txn_count >= 5 and (zero_amount_txns >= 3);

select userID, deviceID, platform, osVer, deviceModel, userIP
from transaction 
where userID in ('0446c341a9756cede989fb03a30203a8', 'acd6b370d43843dc12aab97dfa39583e','4219acfb75a28586e9d86f727badefe1',
'e0c979e16e7cf1eedf92846ccc1bc789','9a1256c59ef6c5b0fde46b6024c9d119',
'477db45ec864a1bfb73a7c08572b297b','d48fe5ad11427ce4422316efe57af8bd','db6b865ee81111bbbae46e3233db71f3');


-- find deviceFingerprints shared across multiple accounts
select
	concat(coalesce(deviceID,''),'_', coalesce(platform,''), '_', coalesce(osVer,''),'_', coalesce(deviceModel,'')) as deviceFingerprint,
    count(distinct userID) as user_count,
    count(*) as total_txns,
    min(cashbackTime) as first_cashback,
    max(cashbackTime) as last_cashback
from transaction
group by deviceFingerprint
having user_count >= 3
order by user_count desc;

-- check on platform 1
select 
	userID,
    userIP,
    campaignID,
    userChargeAmount,
    reqDate,
    cashbackTime
from transaction 
where concat(coalesce(deviceID,''),'_', coalesce(platform, ''),'_', coalesce(osVer,''),'_',coalesce(deviceModel,'')) = '_platform1__'
order by reqDate;
   
select
	userIP,
    count(distinct userID) as num_users,
    sum(userChargeAmount) as total_charged,
    min(reqDate) as first_req,
	max(reqDate) as last_req
from transaction
where concat(coalesce(deviceID,''),'_',coalesce(platform,''),'_',coalesce(osVer,''),'_',coalesce(deviceModel,'')) ='_platform1__'
group by userIP
order by total_charged desc;

-- check on non-platform
select 
	userID,
    userIP,
    campaignID,
    userChargeAmount,
    reqDate,
    cashbackTime
from transaction 
where concat(coalesce(deviceID,''),'_', coalesce(platform, ''),'_', coalesce(osVer,''),'_',coalesce(deviceModel,'')) = '___'
order by reqDate;

select
	userIP,
    count(distinct userID) as num_users,
    sum(userChargeAmount) as total_charged,
    min(reqDate) as first_req,
	max(reqDate) as last_req
from transaction
where concat(coalesce(deviceID,''),'_',coalesce(platform,''),'_',coalesce(osVer,''),'_',coalesce(deviceModel,'')) ='___'
group by userIP
order by total_charged desc;

select 
	t.userID,
    str_to_date(t.reqDate, '%m/%d/%Y') as parsed_reqDate,
    u.created_date,
    case
		when str_to_date(t.reqDate, '%m/%d/%Y') = u.created_date then 'match'
        else 'no match'
	end as date_match_status
from transaction t
join user_profile u on t.userID = u.userID;

-- in map_card
select * from map_card;
-- check if the same users repeated mapping request
select *
from (select 
	userID,
    count(*) as total_requests,
    sum(case when requestStatus = 1 then 1 else 0 end) as successful_requests,
	sum(case when requestStatus <> 1 then 1 else 0 end) as failed_requests
from map_card
group by userID
having count(*) >=5 and sum(case when requestStatus = 1 then 1 else 0 end) = 0 ) as sub 
order by failed_requests asc;

-- check if many users are requesting mapping in many times per day
select
	userID,
    reqDate, 
    count(*) as total_requests,
    sum(case when requestStatus = 1 then 1 else 0 end) as failed_requests
from m 	p_card
group by userID, reqDate
having count(*) >= 3 and sum(case when requestStatus = 1 then 1 else 0 end) = 0
order by failed_requests asc;

-- check if there are bot abuses while using or being attacked bimID
select
	bimID,
    count(distinct userID) as user_count,
    sum(case when requestStatus = 1 then 1 else 0 end) as success_count
from map_card
group by bimID
having user_count >= 2 and success_count = 0 
order by user_count asc;

select * from user_profile;

-- check if shared devices in user profile
select phone_provider, count(distinct userID) as user_count
from user_profile
group by phone_provider
having count(distinct userID) >= 5;

with filtered_provider as (
	select phone_provider, count(distinct userID) as user_count
    from user_profile 
    group by phone_provider
    having count(distinct userID) >5 
),
shared_device as
	(select deviceID
    from transaction 
    group by deviceID
    having count(distinct userID)>1
),
user_shared_device as (
	select up.userID, up.phone_provider, count(distinct t.deviceID) as share_device_count
    from user_profile up
    join transaction t on up.userID = t.userID
    join shared_device sd on t.deviceID = sd.deviceID
    join filtered_provider fp on up.phone_provider = fp.phone_provider
    group by up.userID, up.phone_provider
)
select userID, phone_provider, share_device_count
from user_shared_device
order by phone_provider, userID;

select sender, count(distinct receiver) as referred_count
from transfer
group by sender
having count(distinct receiver) > 20 
order by referred_count desc;

select
	sender,
    count(distinct receiver) as referral_count,
    date(reqDate) as transaction_date,
    count(*) as transaction_count,
    sum(amount) as total_amount
from transfer
group by sender, date(reqDate)
having count(distinct receiver) > 20
order by sender, transaction_date desc; -- transfer among  senders/receivers repeatedly

-- detect loop referred patterns
select a.sender as user_a, a.receiver as user_b
from transfer a
join transfer b on a.sender = b.receiver and a.receiver = b.sender; 

-- count unique mutual pairs and estimate loss from loop patterns
with mutual_referrals as (
  select 
    least(a.sender, a.receiver) as user1,
    greatest(a.sender, a.receiver) as user2
  from transfer a
  join transfer b 
    on a.sender = b.receiver
   and a.receiver = b.sender
  group by
    least(a.sender, a.receiver),
    greatest(a.sender, a.receiver)
)
select
  count(*) as mutual_pair_count,
  count(*) * 100000 as estimated_loss
from mutual_referrals;


select  * from referral_mapcard;

select userID, count(distinct refereeId) as user_count
from referral_mapcard
group by userID
having count(distinct refereeId) > 2;

-- check the abnormally high daily referral volumne
select userID, reqDate, count(*) as daily_referral_count
from referral_mapcard
group by userID, reqDate
having daily_referral_count > 10; -- no bot detected

-- check if bot may send referral every single day
select userID, count(distinct reqDate) as active_days
from referral_mapcard
group by userID
having active_days > 3;

------
-- 1. analyze transaction patterns for abuse
with campaign_transactions as (
	select
		t.userID,
        t.deviceID,
        t.userIP,
        t.reqDate,
        t.amount,
        t.discountAmount,
        t.cashbackTime,
        t.campaignID
	from transaction t
    join campaigninfo ci on t.campaignID = ci.campaignID
    where ci.campaignCode = 'ZPI_220801_115'
		and t.transStatus = 1)
	select 
		userID, 
        deviceID,
        userIP,
        count(*) as transaction_count,
        min(reqDate) as first_transaction,
        max(reqDate) as last_transaction,
        sum(amount) as total_amount,
        sum(discountAmount) as total_discount,
        count(distinct cashbackTime) as cashback_count
	from campaign_transactions
    group by userID, deviceID, userIP
    having count(*) >1
    order by transaction_count desc;
    
-- 2. calculate total amount for suspended abusive transactions
with campaign_transactions as (
	select
		t.userID,
        t.deviceID,
        t.userIP,
        t.reqDate,
        t.amount,
        t.discountAmount,
        t.cashbackTime,
        t.campaignID
	from transaction t
    join campaigninfo ci on t.campaignID = ci.campaignID
    where ci.campaignCode = 'ZPI_220802_115'
    and t.transStatus = 1
),
suspected_abuse as (
	select 
		userID,
        count(*) as transaction_amount,
        coalesce(sum(amount),0) as total_amount,
        coalesce(sum(discountAmount),0) as total_discount
	from transaction
    group by userID
    having count(*) > 10)
select
	sum(transaction_amount) as total_abusive_transactions,
    sum(total_amount) as total_abusive_amount,
    sum(total_discount) as total_abusive_discount
from suspected_abuse;

-- 3. check deviceID and IP patterns
select
	deviceID,
    userIP,
    count(distinct userID) as unique_users,
    count(*) as transaction_count
from transaction t
join campaigninfo ci on t.campaignID = ci.campaignID
where ci.campaignCode = 'ZPI_220801_115'
	and transStatus = 1	
group by deviceID, userIP
having count(distinct userID) > 1
order by transaction_count desc;

select userID fro
