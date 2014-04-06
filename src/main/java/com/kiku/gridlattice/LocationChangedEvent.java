package com.kiku.gridlattice;

/**
 * Converts an entry
 * {"eventType": "ENTER", "userId": "Robert_9", "time": 1374916043917, "roomId": "Cafetaria", "id": "5ff7776b8b54de2c64839699b88ec229A", "corrId": "5ff7776b8b54de2c64839699b88ec229"}
 */
public class LocationChangedEvent {

    private EVENT_TYPE eventType;       //"eventType": "ENTER",
    private String userId;              //"userId": "Robert_9",
    private Long time;                  //"time": 1374916043917,
	private String id;                  //"roomId": "Cafetaria",
    private String corrId;              //"id": "5ff7776b8b54de2c64839699b88ec229A",
	private String roomId;              //"corrId": "5ff7776b8b54de2c64839699b88ec229"}

    public enum EVENT_TYPE {
        ENTER, LEAVE
    }

    public String getId() {
		return id;
	}

	public void setId(String eventId) {
		this.id = eventId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public EVENT_TYPE getEventType() {
		return eventType;
	}

	public void setEventType(EVENT_TYPE eventType) {
		this.eventType = eventType;
	}

	public String getRoomId() {
		return roomId;
	}

	public void setRoomId(String roomId) {
		this.roomId = roomId;
	}


	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

    public String getCorrId() {
        return corrId;
    }

    public void setCorrId(String corrId) {
        this.corrId = corrId;
    }


    // event identity is based only on its id
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LocationChangedEvent other = (LocationChangedEvent) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}


}
