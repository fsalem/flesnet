#ifndef _PID_SOURCE_
#define _PID_SOURCE_

#include <iostream>
#include <cmath>
#include "pid.h"

using namespace std;

namespace tl_libfabric
{
class PIDImpl
{
    public:
        PIDImpl( double dt, uint64_t max, uint64_t min, double Kp, double Kd, double Ki );
        ~PIDImpl();
        uint64_t calculate( uint64_t setpoint, uint64_t pv );
        void set_max(uint64_t max){_max=max;}

    private:
        double _dt;
        uint64_t _max;
        uint64_t _min;
        double _Kp;
        double _Kd;
        double _Ki;
        uint64_t _pre_error;
        double _integral;
};


PID::PID( double dt, uint64_t max, uint64_t min, double Kp, double Kd, double Ki )
{
    pimpl = new PIDImpl(dt,max,min,Kp,Kd,Ki);
}
uint64_t PID::calculate( uint64_t setpoint, uint64_t pv )
{
    return pimpl->calculate(setpoint,pv);
}
void PID::set_max(uint64_t max){
	pimpl->set_max(max);
}
PID::~PID() 
{
    delete pimpl;
}


/**
 * Implementation
 */
PIDImpl::PIDImpl( double dt, uint64_t max, uint64_t min, double Kp, double Kd, double Ki ) :
    _dt(dt),
    _max(max),
    _min(min),
    _Kp(Kp),
    _Kd(Kd),
    _Ki(Ki),
    _pre_error(0),
    _integral(0)
{
}

uint64_t PIDImpl::calculate( uint64_t setpoint, uint64_t pv )
{
    
    // Calculate error
	uint64_t error = setpoint - pv;

    // Proportional term
    double Pout = _Kp * error;

    // Integral term
    _integral += error * _dt;
    double Iout = _Ki * _integral;

    // Derivative term
    double derivative = (error - _pre_error) / _dt;
    double Dout = _Kd * derivative;

    // Calculate total output
    uint64_t output = Pout + Iout + Dout;

    // Restrict to max/min
    if( output > _max )
        output = _max;
    else if( output < _min )
        output = _min;

    // Save error to previous error
    _pre_error = error;

    return output;
}

PIDImpl::~PIDImpl()
{
}
}
#endif
