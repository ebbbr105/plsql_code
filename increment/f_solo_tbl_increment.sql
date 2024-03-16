create or replace function schema.f_solo_tbl_increment (p_id numeric)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$


declare
	tbl_name_gp text; 
	tbl_name_external text; 
	max_data_gp text;     
	query text := '';      
	query_insert_1 text := '';             
	attr_gp text;      
	attr_external text;     
	l_a_Cnt int := 0;    
	v_result text;   
	num numeric;       

begin
		       
		select external_table, insert_table
		into tbl_name_external, tbl_name_gp
		from schema.meta_sandbox
		where id = p_id; 

		      
		select string_agg(column_name, ', ' order by ordinal_position)
		into attr_gp 
		from information_schema.columns
		where table_schema = 'schema' and table_name = tbl_name_gp;
		
		      
		select string_agg(column_name, ', ' order by ordinal_position)
		into attr_external 
		from information_schema.columns 
		where table_schema = 'schema' and table_name = tbl_name_external;

		    id_process   
		execute 'SELECT strpos(''' || attr_external || ''', ''id_process'');' into num; 
		
		if num > 0
		then 
			      
			query := 'select max (id_process) from schema.' || tbl_name_gp;
			execute query into max_data_gp; 
		
			    
			query_insert_1  := 'INSERT INTO schema.'|| '"'|| tbl_name_gp || '"' || '(' || attr_gp || ')' || 'select ' || attr_external || ', ''HELLO_UBAD'', ' 
			|| 'CURRENT_TIMESTAMP, ' || '''0'''
		    || ' FROM schema.' || tbl_name_external || ' where id_process > ''' || max_data_gp || ''';'; 
		 
		   	      
			if query_insert_1 is not null 
				then execute query_insert_1;
					get diagnostics l_a_Cnt := ROW_COUNT;
					v_result := '  schema.' || tbl_name_gp || '  ' || l_a_Cnt || ' .';
			else 
				v_result := 'no new data ' || l_a_Cnt;
			end if;
			
			return v_result;
		else 
			    dttm_update    
			execute 'SELECT strpos(''' || attr_external || ''', ''dttm_update'');' into num; 
			if num > 0
				then 
					      
					query := 'select max (dttm_update) from schema.' || tbl_name_gp;
					execute query into max_data_gp;
				
					    
					query_insert_1  := 'INSERT INTO schema.'|| '"'|| tbl_name_gp || '"' || '(' || attr_gp || ')' || 'select ' || attr_external || ', ''HELLO_UBAD'', ' 
					|| 'CURRENT_TIMESTAMP, ' || '''0'''
				    || ' FROM schema.' || tbl_name_external || ' where dttm_update > ''' || max_data_gp || ''';';
				   	
				          
					if query_insert_1 is not null 
						then execute query_insert_1;
							get diagnostics l_a_Cnt := ROW_COUNT;
							v_result := '  schema.' || tbl_name_gp || '  ' || l_a_Cnt || ' .';
					else 
						v_result := 'no new data ' || l_a_Cnt;
					end if;
					
					return v_result;
			else 
				    dttm_insert   
				execute 'SELECT strpos(''' || attr_external || ''', ''dttm_insert'');' into num; 
				if num > 0
					then 
						      
						query := 'select max (dttm_insert) from schema.' || tbl_name_gp;
						execute query into max_data_gp;
					
						    
						query_insert_1  := 'INSERT INTO schema.'|| '"'|| tbl_name_gp || '"' || '(' || attr_gp || ')' || 'select ' || attr_external || ', ''HELLO_UBAD'', ' 
						|| 'CURRENT_TIMESTAMP, ' || '''0'''
					    || ' FROM schema.' || tbl_name_external || ' where dttm_insert > ''' || max_data_gp || ''';';
					   	
					   	      
						if query_insert_1 is not null 
							then execute query_insert_1;
								get diagnostics l_a_Cnt := ROW_COUNT;
								v_result := '  schema.' || tbl_name_gp || '  ' || l_a_Cnt || ' .';
						else 
							v_result := 'no new data ' || l_a_Cnt;
						end if;
						
						return v_result;
				else 
					v_result := ' id_process, dttm_insert, dttm_update    : ' || tbl_name_external;
				end if;
				return v_result;
			end if;
		end if;	
end;
$$
EXECUTE ON ANY;

  
SELECT schema.f_solo_tbl_increment(id);