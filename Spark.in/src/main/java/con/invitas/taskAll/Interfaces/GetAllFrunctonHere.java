package con.invitas.taskAll.Interfaces;

import java.text.ParseException;

public interface GetAllFrunctonHere {
	public void MakeJsonObjectAndReturnItAsString(String DeviceId, String DataTime, String CurrentA, String CurrentB)
			throws ParseException;

	public void ImportJsonDataIntoKafka(String finalData);
}
