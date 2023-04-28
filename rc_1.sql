CREATE SCHEMA device;

CREATE PROCEDURE device.set_messages(IN p_message jsonb, IN p_offset bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
    begin
	    INSERT INTO device.messages (
	    	offset_msg,
            created_id,
            device_id,
            object_id,
            mes_id,
            mes_time,
            mes_code,
            mes_status,
            mes_data,
            event_value,
            event_data
        )
        VALUES (
        	p_offset,
            convert_to((p_message -> 'data' ->> '_id'), 'utf8'),
            (p_message ->> 'device_id') :: bigint,
            (p_message ->> 'object_id') :: integer,
            (p_message ->> 'mes_id') :: bigint,
            (p_message ->> 'mes_time') :: timestamp,
            (p_message -> 'event_info' ->> 'code') :: integer,
            (p_message -> 'data' -> 'status_info'),
            (p_message -> 'data'),
            (p_message -> 'event'),
            (p_message -> 'event_data')
        );
    end;
$$;

CREATE PROCEDURE device.check_section(IN p_message jsonb, IN p_offset bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
    DECLARE 
        object_id integer;
    BEGIN
	    begin
		    call device.set_messages(p_message, p_offset);
		EXCEPTION
			when check_violation
			then
                object_id = (p_message->>'object_id')::integer;
				EXECUTE format('CREATE TABLE %s PARTITION OF device.messages FOR VALUES IN (%s)',
                  'device.message_' || object_id, object_id);
				call device.set_messages(p_message, p_offset);
	    end;
   end;
$$;

CREATE TABLE device.messages (
    id        bigint generated always as identity,
    offset_msg bigint,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_id bytea,
    device_id bigint,
    object_id integer,
    mes_id bigint,
    mes_time timestamp without time zone,
    mes_code integer,
    mes_status jsonb,
    mes_data jsonb,
    event_value character varying,
    event_data jsonb
)
partition by list(object_id);

CREATE FUNCTION device.get_offset() returns bigint as $$
	select coalesce(
	(select offset_msg
	FROM device.messages 
	ORDER BY created_at 
	DESC LIMIT 1), 0);
$$LANGUAGE SQL;
