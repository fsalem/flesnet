#ifndef _PID_H_
#define _PID_H_

namespace tl_libfabric
{
class PIDImpl;
class PID
{
    public:
        // Kp -  proportional gain
        // Ki -  Integral gain
        // Kd -  derivative gain
        // dt -  loop interval time
        // max - maximum value of manipulated variable
        // min - minimum value of manipulated variable
        PID( double dt, uint64_t max, uint64_t min, double Kp, double Kd, double Ki );

        // Returns the manipulated variable given a setpoint and current process value
        uint64_t calculate( uint64_t setpoint, uint64_t pv );

        void set_max(uint64_t max);
        ~PID();

    private:
        PIDImpl *pimpl;
};
}
#endif
