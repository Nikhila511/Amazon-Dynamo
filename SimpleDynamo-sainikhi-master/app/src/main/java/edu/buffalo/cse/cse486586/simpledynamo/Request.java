package edu.buffalo.cse.cse486586.simpledynamo;

public class Request {
    protected String action;
    protected String present_port;
    protected String neighbors;

    public Request() {
        this.action = null;
        this.present_port = null;
        this.neighbors = null;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getPresent_port() {
        return present_port;
    }

    public void setPresent_port(String present_port) {
        this.present_port = present_port;
    }

    public String getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(String neighbors) {
        this.neighbors = neighbors;
    }

    /*public Request set_params(String req){
        String[] req_params = req.split(":");
        Request
        for (int k = 0; k < req_params.length; k++) {
            if (req_params[k].equals("action")) {
                k = k + 1;
                this.action = req_params[k];
                continue;
            } else if (req_params[k].equals("port")) {
                k++;
                this.present_port = req_params[k];
                continue;
            } else if (req_params[k].equals("msg")) {
                k++;
                this.neighbors = req_params[k];
                continue;
            }
        }
        return
    }*/


    @Override
    public String toString() {
        return "action:"+ action + ":port:" + present_port +":neighbors:" + neighbors + "\n";
    }
}

