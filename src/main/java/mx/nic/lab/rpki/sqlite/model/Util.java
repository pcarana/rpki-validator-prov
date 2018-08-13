package mx.nic.lab.rpki.sqlite.model;

import java.util.LinkedHashMap;
import java.util.Map;

import mx.nic.lab.rpki.db.pojo.PagingParameters;

/**
 * Static util functions useful for common actions
 *
 */
public class Util {

	/**
	 * Return the query with the "[order]" and "[limit]" parameters replaced with
	 * its corresponding values.<br>
	 * The <code>propertyToColumnMap</code> is used to know what column corresponds
	 * to each key in <code>sort</code> map.
	 * 
	 * @param query
	 *            The query where the sorting and limit will be applied
	 * @param pagingParams
	 *            {@link PagingParameters} where <code>null</code> means that no
	 *            limit, no offset, no sort is desired. Otherwise the properties
	 *            mean:
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
			return query.replace("[order]", "").replace("[limit]", "");
		}
		StringBuilder sbSort = new StringBuilder();
		StringBuilder sbLimit = new StringBuilder();
		int limit = pagingParams.getLimit();
		int offset = pagingParams.getOffset();
		LinkedHashMap<String, String> sort = pagingParams.getSort();
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

		return query.replace("[order]", sbSort.toString()).replace("[limit]", sbLimit.toString());
	}
}
