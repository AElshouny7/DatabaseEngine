package main;

public class SQLTerm {
	private String _strTableName;
	private String _strColumnName;
	private String _strOperator;
	private Object _objValue;

	public String get_strTableName() {
		return _strTableName;
	}

	public String get_strColumnName() {
		return _strColumnName;
	}

	public String get_strOperator() {
		return _strOperator;
	}

	public Object get_objValue() {
		return _objValue;
	}

	public void set_strTableName(String _strTableName) {
		this._strTableName = _strTableName;
	}

	public void set_strColumnName(String _strColumnName) {
		this._strColumnName = _strColumnName;
	}

	public void set_strOperator(String _strOperator) {
		this._strOperator = _strOperator;
	}

	public void set_objValue(Object _objValue) {
		this._objValue = _objValue;
	}

	public boolean checkCondition(Object columnValue) {
		switch (_strOperator) {
			case "=":
				return columnValue.equals(_objValue);
			case "!=":
				return !columnValue.equals(_objValue);
			case "<":
				return ((ComparableValue) columnValue).compare((ComparableValue) _objValue) < 0;
			case ">":
				return ((ComparableValue) columnValue).compare((ComparableValue) _objValue) > 0;
			case "<=":
				return ((ComparableValue) columnValue).compare((ComparableValue) _objValue) <= 0;
			case ">=":
				return ((ComparableValue) columnValue).compare((ComparableValue) _objValue) >= 0;
			default:
				throw new IllegalArgumentException("Unsupported operator: " + _strOperator);
		}
	}

}
