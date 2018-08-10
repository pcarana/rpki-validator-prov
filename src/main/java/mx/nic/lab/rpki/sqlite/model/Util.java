package mx.nic.lab.rpki.sqlite.model;

import java.util.LinkedHashMap;
import java.util.Map;

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
	 * @param propertyToColumnMap
	 *            The mapping of the POJO properties to the corresponding DB columns
	 * @param query
	 *            The query where the sorting and limit will be applied
	 * @param limit
	 *            Desired limit to the query, it translates into a LIMIT statement
	 *            (if less than or equal to 0 then it's ignored, as well as the
	 *            offset)
	 * @param offset
	 *            Desired offset to the query, placed as an OFFSET statement after
	 *            the LIMIT (if less than 0 then it's ignored)
	 * @param sort
	 *            Columns used for sorting, the order does matter; the key at the
	 *            {@link LinkedHashMap} is the POJOs property and the value is the
	 *            ordering term (asc or desc)
	 * @return the updated query with the specified parameters
	 */
	public static String getQueryWithPaging(String query, int limit, int offset, LinkedHashMap<String, String> sort,
			Map<String, String> propertyToColumnMap) {
		StringBuilder sbSort = new StringBuilder();
		StringBuilder sbLimit = new StringBuilder();
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
