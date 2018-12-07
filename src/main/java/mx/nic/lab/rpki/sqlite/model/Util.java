package mx.nic.lab.rpki.sqlite.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import mx.nic.lab.rpki.db.pojo.PagingParameters;

/**
 * Static util functions useful for common actions
 *
 */
public class Util {

	/**
	 * Character that will be used at SQL queries where a 'like' operator is used
	 */
	public static final char SQL_ESCAPE_CHAR = '\\';

	/**
	 * Return the query with the "[filter]", "[order]" and "[limit]" parameters
	 * replaced with its corresponding values.<br>
	 * The <code>propertyToColumnMap</code> is used to know what column corresponds
	 * to each key in <code>sort</code> map.
	 * 
	 * @param query
	 *            The query where the filter, sorting and limit will be applied
	 * @param pagingParams
	 *            {@link PagingParameters} where <code>null</code> means that no
	 *            filter, no limit, no offset, no sort is desired. Otherwise the
	 *            properties mean:
	 *            <li>filter: Desired filter to apply to the query, it translates
	 *            into a LIKE statement, leaving a parameter ('?') that must be
	 *            replaced with {@link PagingParameters#getFilterQuery()} value
	 *            <li>limit: Desired limit to the query, it translates into a LIMIT
	 *            statement (if less than or equal to 0 then it's ignored, as well
	 *            as the offset)
	 *            <li>offset: Desired offset to the query, placed as an OFFSET
	 *            statement after the LIMIT (if less than 0 then it's ignored)
	 *            <li>sort: Columns used for sorting, the order does matter; the key
	 *            at the {@link LinkedHashMap} is the POJOs property and the value
	 *            is the ordering term (asc or desc)
	 * @param propertyToColumnMap
	 *            The mapping of the POJO properties to the corresponding DB columns
	 * @return the updated query with the specified parameters
	 */
	public static String getQueryWithPaging(String query, PagingParameters pagingParams,
			Map<String, String> propertyToColumnMap) {
		if (pagingParams == null) {
			return query.replace("[filter]", "").replace("[order]", "").replace("[limit]", "");
		}
		StringBuilder sbFilter = new StringBuilder();
		StringBuilder sbSort = new StringBuilder();
		StringBuilder sbLimit = new StringBuilder();
		String filterColumn = pagingParams.getFilterField();
		int limit = pagingParams.getLimit();
		int offset = pagingParams.getOffset();
		LinkedHashMap<String, String> sort = pagingParams.getSort();
		if (filterColumn != null && pagingParams.getFilterQuery() != null) {
			sbFilter.append(" and ");
			sbFilter.append(propertyToColumnMap.get(filterColumn));
			sbFilter.append(" like ? escape '");
			sbFilter.append(SQL_ESCAPE_CHAR);
			sbFilter.append("' ");
		}
		if (sort != null && !sort.isEmpty()) {
			sbSort.append(" order by ");
			for (String prop : sort.keySet()) {
				sbSort.append(propertyToColumnMap.get(prop));
				sbSort.append(" ");
				sbSort.append(sort.get(prop));
				sbSort.append(", ");
			}
			sbSort.delete(sbSort.length() - 2, sbSort.length());
			sbSort.append(" ");
		}
		if (limit > 0) {
			sbLimit.append(" limit ").append(limit);
			if (offset >= 0) {
				sbLimit.append(" offset ").append(offset);
			}
			sbLimit.append(" ");
		}

		return query.replace("[filter]", sbFilter.toString()).replace("[order]", sbSort.toString()).replace("[limit]",
				sbLimit.toString());
	}

	/**
	 * Set the filter parameter value, if present in the <code>pagingParams</code>,
	 * at the <code>statement</code>, using the index <code>filterIndex</code>
	 * 
	 * @param pagingParams
	 * @param statement
	 * @param filterIndex
	 * @throws SQLException
	 */
	public static void setFilterParam(PagingParameters pagingParams, PreparedStatement statement, int filterIndex)
			throws SQLException {
		if (pagingParams != null && pagingParams.getFilterField() != null) {
			statement.setString(filterIndex, getCleanLikeString(pagingParams.getFilterQuery()));
		}
	}

	/**
	 * Get the SQL 'like' escaping undesired chars (%, _, and
	 * {@link Util#SQL_ESCAPE_CHAR})
	 * 
	 * @param value
	 * @return
	 */
	private static String getCleanLikeString(String value) {
		value = value.replace("%", SQL_ESCAPE_CHAR + "%");
		value = value.replace("_", SQL_ESCAPE_CHAR + "_");
		value = value.replace("" + SQL_ESCAPE_CHAR, SQL_ESCAPE_CHAR + "" + SQL_ESCAPE_CHAR);
		return "%".concat(value).concat("%");
	}
}
